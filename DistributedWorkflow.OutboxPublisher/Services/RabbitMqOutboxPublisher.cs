using System.Diagnostics;
using System.Text;
using DistributedWorkflow.OutboxPublisher.Entities;
using DistributedWorkflow.OutboxPublisher.Metrics;
using RabbitMQ.Client;

namespace DistributedWorkflow.OutboxPublisher.Services;

public sealed class RabbitMqOutboxPublisher(
    RabbitMqConnectionProvider connectionProvider) : IAsyncDisposable
{
    private const string ExchangeName = "registration.commands";
    private const string QueueName = "registration.register";
    private const string RoutingKey = "registration.register";

    private readonly SemaphoreSlim _publishLock = new(1, 1);

    private IChannel? _channel;

    public async Task<IReadOnlyList<OutboxPublishResult>> PublishBatchAsync(
        IReadOnlyList<OutboxPublishRequest> messages,
        CancellationToken cancellationToken = default)
    {
        if (messages.Count == 0)
        {
            return Array.Empty<OutboxPublishResult>();
        }

        await _publishLock.WaitAsync(cancellationToken);
        var startedAt = Stopwatch.GetTimestamp();

        OutboxMetrics.PublishBatchSize.Record(messages.Count);

        try
        {
            var channel = await GetChannelAsync(cancellationToken);
            var publishOperations =
                new List<(Guid MessageId, ValueTask PublishTask)>(
                    messages.Count);

            foreach (var message in messages)
            {
                var body = Encoding.UTF8.GetBytes(message.Payload);

                var properties = new BasicProperties
                {
                    Persistent = true,
                    MessageId = message.MessageId.ToString(),
                    ContentType = "application/json",
                    Type = message.MessageType,
                    Timestamp = new AmqpTimestamp(
                        DateTimeOffset.UtcNow.ToUnixTimeSeconds())
                };

                var publishTask = channel.BasicPublishAsync(
                    exchange: ExchangeName,
                    routingKey: RoutingKey,
                    mandatory: true,
                    basicProperties: properties,
                    body: body,
                    cancellationToken: cancellationToken);
                publishOperations.Add((message.MessageId, publishTask));
            }

            var results = new List<OutboxPublishResult>(
                publishOperations.Count);

            foreach (var publishOperation in publishOperations)
            {
                try
                {
                    await publishOperation.PublishTask;

                    results.Add(
                        new OutboxPublishResult(
                            publishOperation.MessageId,
                            true,
                            null));
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    results.Add(
                        new OutboxPublishResult(
                            publishOperation.MessageId,
                            false,
                            exception));
                }
            }

            if (!channel.IsOpen)
            {
                await ResetChannelAsync();
            }

            return results;
        }
        catch
        {
            await ResetChannelAsync();
            throw;
        }
        finally
        {
            OutboxMetrics.PublishBatchDuration.Record(
                Stopwatch.GetElapsedTime(startedAt).TotalSeconds);
            _publishLock.Release();
        }
    }

    private async Task<IChannel> GetChannelAsync(
        CancellationToken cancellationToken)
    {
        if (_channel is { IsOpen: true })
        {
            return _channel;
        }

        await ResetChannelAsync();

        var connection = await connectionProvider.GetConnectionAsync(
            cancellationToken);

        _channel = await connection.CreateChannelAsync(
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
            // The next publish creates a new channel.
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
        }
        finally
        {
            _publishLock.Release();
            _publishLock.Dispose();
        }
    }
}
