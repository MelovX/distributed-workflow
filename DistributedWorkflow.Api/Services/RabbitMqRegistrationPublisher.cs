using DistributedWorkflow.Api.Configuration;
using DistributedWorkflow.Api.Models;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;


namespace DistributedWorkflow.Api.Services
{
    public sealed class RabbitMqRegistrationPublisher : IAsyncDisposable
    {
        private const string ExchangeName = "registration.commands";
        private const string QueueName = "registration.register";
        private const string RoutingKey = "registration.register";

        private readonly ConnectionFactory _factory;
        private readonly SemaphoreSlim _publishLock = new(1, 1);

        private IConnection? _connection;
        private IChannel? _channel;

        public RabbitMqRegistrationPublisher(IOptions<RabbitMqOptions> options)
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

        public async Task PublishRawAsync(string messageId, string payload,
            CancellationToken cancellationToken = default)
        {
            await _publishLock.WaitAsync(cancellationToken);

            try
            {
                var channel = await GetChannelAsync(cancellationToken);
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
            catch
            {
                await ResetChannelAsync();
                throw;
            }
            finally
            {
                _publishLock.Release();
            }
        }

        private async Task<IChannel> GetChannelAsync(CancellationToken cancellationToken)
        {
            if (_channel is { IsOpen: true })
            {
                return _channel;
            }

            await ResetChannelAsync();

            if (_connection is not { IsOpen: true })
            {
                if (_connection is not null)
                {
                    await _connection.DisposeAsync();
                }

                _connection = await _factory.CreateConnectionAsync(cancellationToken);
            }

            _channel = await _connection.CreateChannelAsync(
                new CreateChannelOptions(
                    publisherConfirmationsEnabled: true,
                    publisherConfirmationTrackingEnabled: true),
                cancellationToken);

            await DeclareTopologyAsync(_channel, cancellationToken);

            return _channel;
        }

        private async Task ResetChannelAsync()
        {
            if (_channel is null)
            {
                return;
            }

            try
            {
                await _channel.DisposeAsync();
            }
            catch
            {
                // The channel is already unusable. The next publish creates a new one.
            }
            finally
            {
                _channel = null;
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
            await _publishLock.WaitAsync();

            try
            {
                await ResetChannelAsync();

                if (_connection is not null)
                {
                    await _connection.DisposeAsync();
                    _connection = null;
                }
            }
            finally
            {
                _publishLock.Release();
                _publishLock.Dispose();
            }
        }
    }
}
