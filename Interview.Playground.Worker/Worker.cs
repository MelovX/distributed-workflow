using Interview.Playground.Numbering.Grpc;
using Interview.Playground.Worker.Configuration;
using Interview.Playground.Worker.Data;
using Interview.Playground.Worker.Events;
using Interview.Playground.Worker.Models;
using Interview.Playground.Worker.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client.Exceptions;

namespace Interview.Playground.Worker;

public sealed class Worker : BackgroundService
{
    private const string _exchangeName = "registration.commands";
    private const string _queueName = "registration.register";
    private const string _routingKey = "registration.register";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly KafkaDocumentEventPublisher _kafkaPublisher;
    private readonly NumberingService.NumberingServiceClient _numberingClient;
    private readonly RabbitMqOptions _rabbitMqOptions;
    private readonly ILogger<Worker> _logger;

    private static readonly TimeSpan RabbitMqRetryDelay = TimeSpan.FromSeconds(5);

    private IConnection? _connection;
    private IChannel? _channel;

    public Worker(
        IServiceScopeFactory scopeFactory, 
        KafkaDocumentEventPublisher documentEventPublisher,
        NumberingService.NumberingServiceClient numberingServiceClient,
        IOptions<RabbitMqOptions> rabbitMqOptions,
        ILogger<Worker> logger)
    {
        _scopeFactory = scopeFactory;
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
            ClientProvidedName = "interview-registration-worker"
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
            prefetchCount: 1,
            global: false,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.ReceivedAsync += async (_, eventArgs) =>
        {
            try
            {
                var json = Encoding.UTF8.GetString(eventArgs.Body.ToArray());

                var command = JsonSerializer.Deserialize<RegisterDocumentCommand>(json);

                if (command is null)
                {
                    Console.WriteLine("Invalid register document command.");

                    await _channel.BasicRejectAsync(
                        deliveryTag: eventArgs.DeliveryTag,
                        requeue: false,
                        cancellationToken: stoppingToken);

                    return;
                }

                await using var scope = _scopeFactory.CreateAsyncScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<RegistrationDbContext>();

                var operation = await dbContext.RegistrationOperations
                    .FirstOrDefaultAsync(x => x.Id == command.OperationId, stoppingToken);

                if (operation is null)
                {
                    Console.WriteLine($"Operation not found. OperationId={command.OperationId}");

                    await _channel.BasicRejectAsync(
                        deliveryTag: eventArgs.DeliveryTag,
                        requeue: false,
                        cancellationToken: stoppingToken);

                    return;
                }

                if (operation.Status == "Succeeded")
                {
                    Console.WriteLine($"Operation already succeeded. OperationId={command.OperationId}");

                    await _channel.BasicAckAsync(
                        deliveryTag: eventArgs.DeliveryTag,
                        multiple: false,
                        cancellationToken: stoppingToken);

                    return;
                }

                operation.Status = "InProgress";
                await dbContext.SaveChangesAsync(stoppingToken);

                Console.WriteLine(
                    $"Registration started. OperationId={command.OperationId}, DocumentId={command.DocumentId}, Title={command.Title}");

                var reserveNumberResponse = await _numberingClient.ReserveNumberAsync(
                    new ReserveNumberRequest
                    {
                        DocumentId = command.DocumentId,
                        OperationId = command.OperationId
                    },
                    deadline: DateTime.UtcNow.AddSeconds(3),
                    cancellationToken: stoppingToken);

                Console.WriteLine(
                    $"Number reserved via gRPC. Number={reserveNumberResponse.Number}, Status={reserveNumberResponse.Status}");

                operation.Status = "Succeeded";
                await dbContext.SaveChangesAsync(stoppingToken);

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

                Console.WriteLine(
                    $"DocumentRegisteredEvent published. OperationId={command.OperationId}");

                await _channel.BasicAckAsync(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false,
                    cancellationToken: stoppingToken);

                Console.WriteLine(
                    $"Registration completed. OperationId={command.OperationId}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Registration failed: {ex.Message}");

                await _channel.BasicRejectAsync(
                    deliveryTag: eventArgs.DeliveryTag,
                    requeue: false,
                    cancellationToken: CancellationToken.None);
            }
        };

        await _channel.BasicConsumeAsync(
            queue: _queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        Console.WriteLine("Registration worker started.");

        await Task.Delay(Timeout.Infinite, stoppingToken);
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