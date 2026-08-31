using DistributedWorkflow.Numbering.Grpc;
using DistributedWorkflow.Worker.Configuration;
using DistributedWorkflow.Worker.Events;
using DistributedWorkflow.Worker.Metrics;
using DistributedWorkflow.Worker.Models;
using DistributedWorkflow.Worker.Services;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace DistributedWorkflow.Worker;

public sealed class Worker : BackgroundService
{
    private const string _exchangeName = "registration.commands";
    private const string _queueName = "registration.register";
    private const string _routingKey = "registration.register";
    private readonly KafkaDocumentEventPublisher _kafkaPublisher;
    private readonly NumberingService.NumberingServiceClient _numberingClient;
    private readonly RabbitMqOptions _rabbitMqOptions;
    private readonly ILogger<Worker> _logger;

    private static readonly TimeSpan RabbitMqRetryDelay = TimeSpan.FromSeconds(5);
    private readonly SemaphoreSlim _acknowledgementLock = new(1, 1);

    private IConnection? _connection;
    private IChannel? _channel;

    public Worker(
        KafkaDocumentEventPublisher documentEventPublisher,
        NumberingService.NumberingServiceClient numberingServiceClient,
        IOptions<RabbitMqOptions> rabbitMqOptions,
        ILogger<Worker> logger)
    {
        _kafkaPublisher = documentEventPublisher;
        _numberingClient = numberingServiceClient;
        _rabbitMqOptions = rabbitMqOptions.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
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

        _connection = await ConnectWithRetryAsync(factory, stoppingToken);
        _channel = await _connection.CreateChannelAsync(cancellationToken: stoppingToken);

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

        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.ReceivedAsync += async (_, eventArgs) =>
        {
            var startedAt = Stopwatch.GetTimestamp();

            try
            {
                var json = Encoding.UTF8.GetString(eventArgs.Body.ToArray());

                var command = JsonSerializer.Deserialize<RegisterDocumentCommand>(json);

                if (command is null)
                {
                    _logger.LogWarning("Invalid register document command.");

                    await RejectAsync(
                        eventArgs.DeliveryTag,
                        stoppingToken);

                    return;
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

                var kafkaStartedAt = Stopwatch.GetTimestamp();

                try
                {
                    await _kafkaPublisher.PublishAsync(
                        new DocumentRegisteredEvent
                        {
                            EventId = Guid.NewGuid().ToString("N"),
                            OperationId = command.OperationId,
                            DocumentId = command.DocumentId,
                            Title = command.Title,
                            RegisteredAt = DateTimeOffset.UtcNow
                        },
                        stoppingToken);
                }
                finally
                {
                    WorkerMetrics.KafkaPublishDuration.Record(
                        Stopwatch.GetElapsedTime(kafkaStartedAt).TotalSeconds);
                }

                _logger.LogDebug(
                    "DocumentRegisteredEvent published. OperationId={OperationId}",
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
            catch (Exception ex)
            {
                _logger.LogError(ex, "Registration failed: DeliveryTag={DeliveryTag}", eventArgs.DeliveryTag);

                await RejectAsync(
                    eventArgs.DeliveryTag,
                    CancellationToken.None);
            }
            finally
            {
                var elapsed = Stopwatch.GetElapsedTime(startedAt);

                WorkerMetrics.RegistrationProcessingDuration.Record(
                    elapsed.TotalSeconds);
            }
        };

        await _channel.BasicConsumeAsync(
            queue: _queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation("Registration worker started.");

        await Task.Delay(Timeout.Infinite, stoppingToken);
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

    private async Task<IConnection> ConnectWithRetryAsync(
        ConnectionFactory factory,
        CancellationToken cancellationToken)
    {
        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            attempt++;

            try
            {
                _logger.LogInformation(
                    "Connecting to RabbitMQ. Attempt {Attempt}.",
                    attempt);

                var connection =
                    await factory.CreateConnectionAsync(cancellationToken);

                _logger.LogInformation(
                    "Connected to RabbitMQ on attempt {Attempt}.",
                    attempt);

                return connection;
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (BrokerUnreachableException exception)
            {
                _logger.LogWarning(
                    exception,
                    "RabbitMQ is unavailable. Retrying in {DelaySeconds} seconds.",
                    RabbitMqRetryDelay.TotalSeconds);

                await Task.Delay(
                    RabbitMqRetryDelay,
                    cancellationToken);
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_channel is not null)
        {
            await _channel.DisposeAsync();
        }

        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }

        await base.StopAsync(cancellationToken);
    }
}
