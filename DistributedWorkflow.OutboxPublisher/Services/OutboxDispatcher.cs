using DistributedWorkflow.OutboxPublisher.Data;
using DistributedWorkflow.OutboxPublisher.Metrics;
using Microsoft.EntityFrameworkCore;

namespace DistributedWorkflow.OutboxPublisher.Services;

public sealed class OutboxDispatcher(
    IServiceScopeFactory scopeFactory,
    RabbitMqOutboxPublisher publisher,
    ILogger<OutboxDispatcher> logger) : BackgroundService
{
    private const int BatchSize = 100;
    private static readonly TimeSpan LockDuration = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan EmptyBatchDelay = TimeSpan.FromSeconds(5);

    private readonly string _dispatcherId =
        $"{Environment.MachineName}-{Guid.NewGuid():N}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "Outbox dispatcher {DispatcherId} started.",
            _dispatcherId);

        while (!stoppingToken.IsCancellationRequested)
        {
            var processedCount = 0;

            try
            {
                processedCount = await DispatchBatchAsync(stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(
                    exception,
                    "Outbox dispatcher iteration failed.");
            }

            if (processedCount == 0)
            {
                await Task.Delay(EmptyBatchDelay, stoppingToken);
            }
        }
    }

    private async Task<int> DispatchBatchAsync(
        CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var dbContext = scope.ServiceProvider
            .GetRequiredService<OutboxDbContext>();

        var now = DateTimeOffset.UtcNow;
        var lockUntil = now.Add(LockDuration);
        var lockOwner = $"{_dispatcherId}-{Guid.NewGuid():N}";

        await using var transaction =
            await dbContext.Database.BeginTransactionAsync(cancellationToken);

        var messages = await dbContext.OutboxMessages
            .FromSqlInterpolated($"""
                SELECT *
                FROM "OutboxMessages"
                WHERE "PublishedAt" IS NULL
                  AND ("LockedUntil" IS NULL OR "LockedUntil" < {now})
                ORDER BY "CreatedAt", "Id"
                LIMIT {BatchSize}
                FOR UPDATE SKIP LOCKED
                """)
            .ToListAsync(cancellationToken);

        if (messages.Count == 0)
        {
            await transaction.CommitAsync(cancellationToken);
            return 0;
        }

        foreach (var message in messages)
        {
            message.LockedUntil = lockUntil;
            message.LockedBy = lockOwner;
        }

        await dbContext.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        foreach (var message in messages)
        {
            try
            {
                await publisher.PublishRawAsync(
                    messageId: message.Id.ToString(),
                    messageType: message.Type,
                    payload: message.Payload,
                    cancellationToken: cancellationToken);

                message.PublishedAt = DateTimeOffset.UtcNow;
                message.LockedUntil = null;
                message.LockedBy = null;
                message.LastError = null;

                OutboxMetrics.Published.Add(1);
            }
            catch (Exception exception)
            {
                message.Attempts += 1;
                message.LastError = exception.Message;

                logger.LogError(
                    exception,
                    "Failed to publish outbox message {MessageId}. Attempt {Attempt}.",
                    message.Id,
                    message.Attempts);

                OutboxMetrics.Failed.Add(1);
            }
        }

        await dbContext.SaveChangesAsync(cancellationToken);

        logger.LogInformation(
            "Processed {Count} outbox messages.",
            messages.Count);

        return messages.Count;
    }
}
