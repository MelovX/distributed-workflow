using DistributedWorkflow.Numbering.Grpc;
using DistributedWorkflow.Worker.Configuration;
using DistributedWorkflow.Worker.Data;
using DistributedWorkflow.Worker.Events;
using DistributedWorkflow.Worker.Metrics;
using DistributedWorkflow.Worker.Models;
using DistributedWorkflow.Worker.Services;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace DistributedWorkflow.Worker;

public sealed class Worker : BackgroundService
{
    private const string _exchangeName = "registration.commands";
    private const string _queueName = "registration.register";
    private const string _routingKey = "registration.register";
    private const string _retry1SecondRoutingKey = "registration.register.retry.1s";
    private const string _retry10SecondsRoutingKey = "registration.register.retry.10s";
    private const string _retry60SecondsRoutingKey = "registration.register.retry.60s";
    private const int _attemptsLimit = 10;

    private readonly NumberingService.NumberingServiceClient _numberingClient;
    private readonly RabbitMqOptions _rabbitMqOptions;
    private readonly ILogger<Worker> _logger;
    private readonly WorkerInboxStore _inboxStore;
    private readonly RabbitMqRetryPublisher _retryPublisher;
    private readonly WorkerInboxBatcher _inboxBatcher;

    private static readonly TimeSpan RabbitMqRetryDelay = TimeSpan.FromSeconds(5);
    private readonly SemaphoreSlim _acknowledgementLock = new(1, 1);

    private IConnection? _connection;
    private IChannel? _channel;

    public Worker(
        NumberingService.NumberingServiceClient numberingServiceClient,
        IOptions<RabbitMqOptions> rabbitMqOptions,
        WorkerInboxStore inboxStore,
        RabbitMqRetryPublisher retryPublisher,
        WorkerInboxBatcher inboxBatcher,
        ILogger<Worker> logger)
    {
        _numberingClient = numberingServiceClient;
        _rabbitMqOptions = rabbitMqOptions.Value;
        _inboxStore = inboxStore;
        _retryPublisher = retryPublisher;
        _inboxBatcher = inboxBatcher;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await ConnectToRabbitMQAsync(stoppingToken);

        var channel = _channel
            ?? throw new InvalidOperationException(
                "RabbitMQ channel was not initialized.");

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, eventArgs) =>
        {
            var startedAt = Stopwatch.GetTimestamp();

            Guid? claimedMessageId = null;
            string? lockOwner = null;

            try
            {
                if (!TryParseCommand(eventArgs.Body, out var command, out var error))
                {
                    _logger.LogWarning(
                        "Invalid register document command. Error={ParsingError}",
                        error);

                    await RejectAsync(
                        eventArgs.DeliveryTag,
                        stoppingToken);

                    return;
                }

                if (!Guid.TryParse(
                    eventArgs.BasicProperties.MessageId,
                    out var messageId))
                {
                    _logger.LogWarning(
                        "RabbitMQ message has invalid MessageId. MessageId={MessageId}",
                        eventArgs.BasicProperties.MessageId);

                    await RejectAsync(
                        eventArgs.DeliveryTag,
                        stoppingToken);

                    return;
                }

                claimedMessageId = messageId;
                lockOwner = Guid.NewGuid().ToString("N");

                var claimResult = await _inboxBatcher.ClaimAsync(
                    new InboxClaimEntry(
                        messageId,
                        command.OperationId,
                        nameof(RegisterDocumentCommand),
                        lockOwner),
                    stoppingToken);

                switch (claimResult.Status)
                {
                    case InboxClaimStatus.AlreadyCompleted:
                        await AcknowledgeAsync(
                            eventArgs.DeliveryTag,
                            stoppingToken);
                        return;

                    case InboxClaimStatus.LeaseHeld:
                        await ScheduleRetryAndAcknowledgeAsync(
                            eventArgs,
                            messageId,
                            attempts: null,
                            stoppingToken);
                        return;

                    case InboxClaimStatus.Acquired:
                        break;
                }

                _logger.LogDebug(
                    "Registration started. OperationId={OperationId}, DocumentId={DocumentId}",
                    command.OperationId,
                    command.DocumentId);

                var numberingStartedAt = Stopwatch.GetTimestamp();
                ReserveNumberResponse reserveNumberResponse;

                try
                {
                    reserveNumberResponse = await _numberingClient.ReserveNumberAsync(
                        new ReserveNumberRequest
                        {
                            DocumentId = command.DocumentId,
                            OperationId = command.OperationId
                        },
                        deadline: DateTime.UtcNow.AddSeconds(3),
                        cancellationToken: stoppingToken);
                }
                finally
                {
                    WorkerMetrics.NumberingRequestDuration.Record(
                        Stopwatch.GetElapsedTime(numberingStartedAt).TotalSeconds);
                }

                _logger.LogDebug(
                    "Number reserved via gRPC. OperationId={OperationId}, Status={Status}",
                    command.OperationId,
                    reserveNumberResponse.Status);

                var eventId = Guid.NewGuid();

                var documentRegisteredEvent = new DocumentRegisteredEvent
                {
                    EventId = eventId.ToString("N"),
                    OperationId = command.OperationId,
                    DocumentId = command.DocumentId,
                    Title = command.Title,
                    RegisteredAt = DateTimeOffset.UtcNow
                };

                var outboxEntry = new KafkaOutboxEntry(
                    eventId,
                    command.OperationId,
                    nameof(DocumentRegisteredEvent),
                    command.DocumentId,
                    JsonSerializer.Serialize(documentRegisteredEvent));

                var completionResult =
                    await _inboxBatcher.CompleteAndEnqueueAsync(
                        new InboxCompletionEntry(
                            messageId,
                            lockOwner,
                            outboxEntry),
                        stoppingToken);

                if (!completionResult.Completed)
                {
                    _logger.LogWarning(
                        "Inbox lease was lost before completion. MessageId={MessageId}",
                        messageId);

                    await RequeueAsync(
                        eventArgs.DeliveryTag,
                        stoppingToken);

                    return;
                }

                _logger.LogDebug(
                    "Registration completed and Kafka event added to outbox. OperationId={OperationId}",
                    command.OperationId);

                var acknowledgementStartedAt = Stopwatch.GetTimestamp();

                try
                {
                    await AcknowledgeAsync(
                        eventArgs.DeliveryTag,
                        stoppingToken);
                }
                finally
                {
                    WorkerMetrics.RabbitMqAcknowledgementDuration.Record(
                        Stopwatch.GetElapsedTime(acknowledgementStartedAt).TotalSeconds);
                }

                WorkerMetrics.RegistrationsCompleted.Add(1);

                _logger.LogDebug(
                    "Registration completed. OperationId={OperationId}",
                    command.OperationId);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Registration processing cancelled during worker shutdown. DeliveryTag={DeliveryTag}",
                    eventArgs.DeliveryTag);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Registration failed. DeliveryTag={DeliveryTag}",
                    eventArgs.DeliveryTag);

                int? attempts = null;

                if (claimedMessageId is Guid messageId &&
                    lockOwner is not null)
                {
                    try
                    {
                        attempts = await _inboxStore.FailAsync(
                            messageId,
                            lockOwner,
                            ex.ToString(),
                            CancellationToken.None);
                    }
                    catch (Exception failException)
                    {
                        _logger.LogError(
                            failException,
                            "Failed to release inbox lease. MessageId={MessageId}",
                            messageId);
                    }
                }

                if (attempts is >= _attemptsLimit)
                {
                    _logger.LogError(
                        "Retry limit reached. MessageId={MessageId}, Attempts={Attempts}",
                        claimedMessageId,
                        attempts);

                    await RejectAsync(
                        eventArgs.DeliveryTag,
                        CancellationToken.None);

                    return;
                }

                await ScheduleRetryAndAcknowledgeAsync(
                    eventArgs,
                    claimedMessageId,
                    attempts,
                    CancellationToken.None);
            }
            finally
            {
                var elapsed = Stopwatch.GetElapsedTime(startedAt);

                WorkerMetrics.RegistrationProcessingDuration.Record(
                    elapsed.TotalSeconds);
            }
        };

        await channel.BasicConsumeAsync(
            queue: _queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation("Registration worker started.");

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task RequeueAsync(ulong deliveryTag, CancellationToken cancellationToken)
    {
        await _acknowledgementLock.WaitAsync(cancellationToken);

        try
        {
            await _channel!.BasicRejectAsync(
                deliveryTag: deliveryTag,
                requeue: true,
                cancellationToken: cancellationToken);
        }
        finally
        {
            _acknowledgementLock.Release();
        }
    }

    private async Task AcknowledgeAsync(
        ulong deliveryTag,
        CancellationToken cancellationToken)
    {
        await _acknowledgementLock.WaitAsync(cancellationToken);

        try
        {
            await _channel!.BasicAckAsync(
                deliveryTag: deliveryTag,
                multiple: false,
                cancellationToken: cancellationToken);
        }
        finally
        {
            _acknowledgementLock.Release();
        }
    }

    private async Task RejectAsync(
        ulong deliveryTag,
        CancellationToken cancellationToken)
    {
        await _acknowledgementLock.WaitAsync(cancellationToken);

        try
        {
            await _channel!.BasicRejectAsync(
                deliveryTag: deliveryTag,
                requeue: false,
                cancellationToken: cancellationToken);
        }
        finally
        {
            _acknowledgementLock.Release();
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await ResetRabbitMqAsync();

        await base.StopAsync(cancellationToken);
    }

    private static bool TryParseCommand(
        ReadOnlyMemory<byte> body,
        [NotNullWhen(true)] out RegisterDocumentCommand? command,
        out string error)
    {
        try
        {
            command = JsonSerializer.Deserialize<RegisterDocumentCommand>(
                body.Span);
        }
        catch (JsonException exception)
        {
            command = null;
            error = exception.Message;

            return false;
        }

        if (command is null)
        {
            error = "Payload contains JSON null.";

            return false;
        }

        if (string.IsNullOrWhiteSpace(command.OperationId) ||
            string.IsNullOrWhiteSpace(command.DocumentId) ||
            string.IsNullOrWhiteSpace(command.Title) ||
            command.CreatedAt == default)
        {
            error = "Command contains invalid required fields.";

            return false;
        }

        error = string.Empty;

        return true;
    }

    private static string GetRetryRoutingKey(int attempts)
    {
        return attempts switch
        {
            1 => _retry1SecondRoutingKey,
            2 or 3 => _retry10SecondsRoutingKey,
            _ => _retry60SecondsRoutingKey
        };
    }

    private async Task ScheduleRetryAndAcknowledgeAsync(
        BasicDeliverEventArgs eventArgs,
        Guid? messageId,
        int? attempts,
        CancellationToken cancellationToken)
    {
        var retryRoutingKey =
            GetRetryRoutingKey(attempts ?? 1);

        try
        {
            await _retryPublisher.PublishAsync(
                retryRoutingKey,
                eventArgs.Body,
                eventArgs.BasicProperties,
                cancellationToken);
        }
        catch (Exception retryException)
        {
            _logger.LogError(
                retryException,
                "Failed to schedule RabbitMQ retry. MessageId={MessageId}",
                messageId);

            await RequeueAsync(
                eventArgs.DeliveryTag,
                cancellationToken);

            return;
        }

        await AcknowledgeAsync(
            eventArgs.DeliveryTag,
            cancellationToken);

        _logger.LogWarning(
            "Registration retry scheduled. MessageId={MessageId}, Attempts={Attempts}, RoutingKey={RoutingKey}",
            messageId,
            attempts,
            retryRoutingKey);
    }

    private async Task ConnectToRabbitMQAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _rabbitMqOptions.HostName,
            Port = _rabbitMqOptions.Port,
            UserName = _rabbitMqOptions.UserName,
            Password = _rabbitMqOptions.Password,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
            ClientProvidedName = "distributed-workflow-registration-worker",
            ConsumerDispatchConcurrency = _rabbitMqOptions.ConsumerConcurrency
        };

        var attempt = 0;

        while (true)
        {
            stoppingToken.ThrowIfCancellationRequested();
            attempt++;

            try
            {
                _logger.LogInformation(
                    "Initializing RabbitMQ consumer. Attempt {Attempt}.",
                    attempt);

                _connection =
                    await factory.CreateConnectionAsync(stoppingToken);
                _channel = await _connection.CreateChannelAsync(
                    cancellationToken: stoppingToken);

                await _channel.ExchangeDeclareAsync(
                    exchange: _exchangeName,
                    type: ExchangeType.Direct,
                    durable: true,
                    autoDelete: false,
                    cancellationToken: stoppingToken);

                await _channel.QueueDeclareAsync(
                    queue: _queueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    cancellationToken: stoppingToken);

                await _channel.QueueBindAsync(
                    queue: _queueName,
                    exchange: _exchangeName,
                    routingKey: _routingKey,
                    cancellationToken: stoppingToken);

                await _channel.BasicQosAsync(
                    prefetchSize: 0,
                    prefetchCount: _rabbitMqOptions.ConsumerConcurrency,
                    global: false,
                    cancellationToken: stoppingToken);

                _logger.LogInformation(
                    "RabbitMQ consumer initialized on attempt {Attempt}.",
                    attempt);

                return;
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                await ResetRabbitMqAsync();
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Failed to initialize RabbitMQ consumer. Retrying in {DelaySeconds} seconds.",
                    RabbitMqRetryDelay.TotalSeconds);

                await ResetRabbitMqAsync();
                await Task.Delay(
                    RabbitMqRetryDelay,
                    stoppingToken);
            }
        }
    }

    private async Task ResetRabbitMqAsync()
    {
        if (_channel is not null)
        {
            try
            {
                await _channel.DisposeAsync();
            }
            catch (Exception exception)
            {
                _logger.LogDebug(
                    exception,
                    "Failed to dispose RabbitMQ consumer channel.");
            }
            finally
            {
                _channel = null;
            }
        }

        if (_connection is not null)
        {
            try
            {
                await _connection.DisposeAsync();
            }
            catch (Exception exception)
            {
                _logger.LogDebug(
                    exception,
                    "Failed to dispose RabbitMQ consumer connection.");
            }
            finally
            {
                _connection = null;
            }
        }
    }
}
