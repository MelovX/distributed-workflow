using DistributedWorkflow.Worker.Configuration;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace DistributedWorkflow.Worker.Services;

public sealed class RabbitMqRetryPublisher : IAsyncDisposable
{
    private const string RetryExchangeName =
        "registration.commands.retry";

    private readonly ILogger<RabbitMqRetryPublisher> _logger;
    private readonly ConnectionFactory _connectionFactory;
    private readonly SemaphoreSlim _synchronizationLock = new(1, 1);

    private IConnection? _connection;
    private IChannel? _channel;

    public RabbitMqRetryPublisher(
        IOptions<RabbitMqOptions> options, ILogger<RabbitMqRetryPublisher> logger)
    {
        var rabbitMq = options.Value;

        _connectionFactory = new ConnectionFactory
        {
            HostName = rabbitMq.HostName,
            Port = rabbitMq.Port,
            UserName = rabbitMq.UserName,
            Password = rabbitMq.Password,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
            ClientProvidedName =
                "distributed-workflow-registration-retry-publisher"
        };
        _logger = logger;
    }

    public async Task PublishAsync(
        string routingKey,
        ReadOnlyMemory<byte> body,
        IReadOnlyBasicProperties sourceProperties,
        CancellationToken cancellationToken)
    {
        await _synchronizationLock.WaitAsync(cancellationToken);

        try
        {
            var channel =
                await GetChannelAsync(cancellationToken);

            var retryProperties =
                new BasicProperties(sourceProperties)
                {
                    Persistent = true
                };

            await channel.BasicPublishAsync(
                exchange: RetryExchangeName,
                routingKey: routingKey,
                mandatory: true,
                basicProperties: retryProperties,
                body: body,
                cancellationToken: cancellationToken);
        }
        catch
        {
            await ResetAsync();
            throw;
        }
        finally
        {
            _synchronizationLock.Release();
        }
    }

    private async Task<IChannel> GetChannelAsync(
        CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true } &&
            _channel is { IsOpen: true })
        {
            return _channel;
        }

        await ResetAsync();

        try
        {
            _connection =
                await _connectionFactory.CreateConnectionAsync(
                    cancellationToken);

            _channel = await _connection.CreateChannelAsync(
                new CreateChannelOptions(
                    publisherConfirmationsEnabled: true,
                    publisherConfirmationTrackingEnabled: true),
                cancellationToken);

            await _channel.ExchangeDeclareAsync(
                exchange: RetryExchangeName,
                type: ExchangeType.Direct,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);

            return _channel;
        }
        catch
        {
            await ResetAsync();
            throw;
        }
    }

    private async Task ResetAsync()
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
                    "Failed to dispose RabbitMQ retry publisher channel.");
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
                    "Failed to dispose RabbitMQ retry publisher connection.");
            }
            finally
            {
                _connection = null;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _synchronizationLock.WaitAsync();

        try
        {
            await ResetAsync();
        }
        finally
        {
            _synchronizationLock.Release();
            _synchronizationLock.Dispose();
        }
    }
}