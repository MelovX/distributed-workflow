using System.Text;
using System.Text.Json;
using Interview.Playground.Api.Models;
using RabbitMQ.Client;


namespace Interview.Playground.Api.Services
{
    public sealed class RabbitMqRegistrationPublisher : IAsyncDisposable
    {
        private const string ExchangeName = "registration.commands";
        private const string QueueName = "registration.register";
        private const string RoutingKey = "registration.register";

        private readonly ConnectionFactory _factory;
        private readonly SemaphoreSlim _connectionLock = new(1, 1);

        private IConnection? _connection;

        public RabbitMqRegistrationPublisher()
        {
            _factory = new ConnectionFactory
            {
                Port = 5673,
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };
        }

        public async Task PublishRawAsync(string messageId, string payload,
            CancellationToken cancellationToken = default)
        {
            var connection = await GetConnectionAsync(cancellationToken);

            await using var channel = await connection.CreateChannelAsync(
                cancellationToken: cancellationToken);

            await DeclareTopologyAsync(channel, cancellationToken);

            var body = Encoding.UTF8.GetBytes(payload);

            var properties = new BasicProperties
            {
                Persistent = true,
                MessageId = messageId,
                ContentType = "application/json",
                Type = nameof(RegisterDocumentCommand),
                Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
            };

            await channel.BasicPublishAsync(
                exchange: ExchangeName,
                routingKey: RoutingKey,
                mandatory: true,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken);
        }

        private async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
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

                _connection = await _factory.CreateConnectionAsync(cancellationToken);

                return _connection;
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        private static async Task DeclareTopologyAsync(
            IChannel channel,
            CancellationToken cancellationToken)
        {
            await channel.ExchangeDeclareAsync(
                exchange: ExchangeName,
                type: ExchangeType.Direct,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);

            await channel.QueueDeclareAsync(
                queue: QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                cancellationToken: cancellationToken);

            await channel.QueueBindAsync(
                queue: QueueName,
                exchange: ExchangeName,
                routingKey: RoutingKey,
                cancellationToken: cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            if (_connection is not null)
            {
                await _connection.DisposeAsync();
            }

            _connectionLock.Dispose();
        }
    }
}
