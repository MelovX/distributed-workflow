using DistributedWorkflow.OutboxPublisher.Configuration;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace DistributedWorkflow.OutboxPublisher.Services;

public sealed class RabbitMqConnectionProvider : IAsyncDisposable
{
    private readonly ConnectionFactory _factory;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    private IConnection? _connection;

    public RabbitMqConnectionProvider(IOptions<RabbitMqOptions> options)
    {
        var rabbitMq = options.Value;

        _factory = new ConnectionFactory
        {
            HostName = rabbitMq.HostName,
            Port = rabbitMq.Port,
            UserName = rabbitMq.UserName,
            Password = rabbitMq.Password,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
            ClientProvidedName = "distributed-workflow-outbox-publisher"
        };
    }

    public async Task<IConnection> GetConnectionAsync(
        CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true })
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken);

        try
        {
            if (_connection is { IsOpen: true })
            {
                return _connection;
            }

            if (_connection is not null)
            {
                await _connection.DisposeAsync();
            }

            _connection = await _factory.CreateConnectionAsync(
                cancellationToken);

            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _connectionLock.WaitAsync();

        try
        {
            if (_connection is not null)
            {
                await _connection.DisposeAsync();
                _connection = null;
            }
        }
        finally
        {
            _connectionLock.Release();
            _connectionLock.Dispose();
        }
    }
}
