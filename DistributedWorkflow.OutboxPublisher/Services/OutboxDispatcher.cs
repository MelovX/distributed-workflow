using DistributedWorkflow.OutboxPublisher.Entities;
using DistributedWorkflow.OutboxPublisher.Metrics;
using Npgsql;

namespace DistributedWorkflow.OutboxPublisher.Services;

public sealed class OutboxDispatcher(
    NpgsqlDataSource dataSource,
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
        var claimedBatch = await ClaimBatchAsync(cancellationToken);

        if (claimedBatch.Messages.Count == 0)
        {
            return 0;
        }

        var publishResults = await publisher.PublishBatchAsync(
            claimedBatch.Messages,
            cancellationToken);

        await CompleteBatchAsync(
            claimedBatch.LockOwner,
            publishResults,
            cancellationToken);

        logger.LogInformation(
            "Processed {Count} outbox messages.",
            claimedBatch.Messages.Count);

        return claimedBatch.Messages.Count;
    }

    private async Task<ClaimedBatch> ClaimBatchAsync(
        CancellationToken cancellationToken)
    {
        await using var connection =
                await dataSource.OpenConnectionAsync(cancellationToken);

        var now = DateTimeOffset.UtcNow;
        var lockUntil = now.Add(LockDuration);
        var lockOwner = $"{_dispatcherId}-{Guid.NewGuid():N}";

        await using var command = new NpgsqlCommand(
            """
                WITH candidates AS
                (
                    SELECT "Id"
                    FROM "OutboxMessages"
                    WHERE "PublishedAt" IS NULL
                      AND ("LockedUntil" IS NULL OR "LockedUntil" < @now)
                    ORDER BY "CreatedAt", "Id"
                    LIMIT @batchSize
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE "OutboxMessages" AS messages
                SET "LockedUntil" = @lockUntil,
                    "LockedBy" = @lockOwner
                FROM candidates
                WHERE messages."Id" = candidates."Id"
                RETURNING messages."Id", messages."Type", messages."Payload";
                """,
            connection);

        command.Parameters.AddWithValue("now", now);
        command.Parameters.AddWithValue("batchSize", BatchSize);
        command.Parameters.AddWithValue("lockUntil", lockUntil);
        command.Parameters.AddWithValue("lockOwner", lockOwner);

        var publishRequests = new List<OutboxPublishRequest>(BatchSize);

        await using var reader = await command.ExecuteReaderAsync(
            cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            publishRequests.Add(
                new OutboxPublishRequest(
                    reader.GetGuid(0),
                    reader.GetString(1),
                    reader.GetString(2)));
        }

        return new ClaimedBatch(
            lockOwner,
            publishRequests);
    }

    private async Task CompleteBatchAsync(
        string lockOwner,
        IReadOnlyList<OutboxPublishResult> publishResults,
        CancellationToken cancellationToken)
    {
        if (publishResults.Count == 0)
        {
            return;
        }

        await using var connection =
                await dataSource.OpenConnectionAsync(cancellationToken);

        var messageIds = new Guid[publishResults.Count];
        var confirmations = new bool[publishResults.Count];
        var failureMessages = new string[publishResults.Count];

        for (var index = 0; index < publishResults.Count; index++)
        {
            var publishResult = publishResults[index];

            messageIds[index] = publishResult.MessageId;
            confirmations[index] = publishResult.IsConfirmed;
            failureMessages[index] = publishResult.Failure?.Message
                ?? "RabbitMQ did not confirm the message.";
        }

        var publishedAt = DateTimeOffset.UtcNow;
        var updatedMessageIds = new HashSet<Guid>();


        await using var command = new NpgsqlCommand(
            """
                UPDATE "OutboxMessages" AS messages
                SET "PublishedAt" =
                        CASE
                            WHEN results."IsConfirmed" THEN @publishedAt
                            ELSE messages."PublishedAt"
                        END,
                    "LockedUntil" =
                        CASE
                            WHEN results."IsConfirmed" THEN NULL
                            ELSE messages."LockedUntil"
                        END,
                    "LockedBy" =
                        CASE
                            WHEN results."IsConfirmed" THEN NULL
                            ELSE messages."LockedBy"
                        END,
                    "Attempts" =
                        CASE
                            WHEN results."IsConfirmed" THEN messages."Attempts"
                            ELSE messages."Attempts" + 1
                        END,
                    "LastError" =
                        CASE
                            WHEN results."IsConfirmed" THEN NULL
                            ELSE results."FailureMessage"
                        END
                FROM unnest(
                    @messageIds::uuid[],
                    @confirmations::boolean[],
                    @failureMessages::text[])
                    AS results("Id", "IsConfirmed", "FailureMessage")
                WHERE messages."Id" = results."Id"
                  AND messages."PublishedAt" IS NULL
                  AND messages."LockedBy" = @lockOwner
                RETURNING messages."Id";
                """,
            connection);

        command.Parameters.AddWithValue("publishedAt", publishedAt);
        command.Parameters.AddWithValue("messageIds", messageIds);
        command.Parameters.AddWithValue("confirmations", confirmations);
        command.Parameters.AddWithValue(
            "failureMessages",
            failureMessages);
        command.Parameters.AddWithValue("lockOwner", lockOwner);

        await using var reader = await command.ExecuteReaderAsync(
            cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            updatedMessageIds.Add(reader.GetGuid(0));
        }

        var publishedCount = 0;
        var failedCount = 0;
        var ownershipConflictCount = 0;

        foreach (var publishResult in publishResults)
        {
            if (!updatedMessageIds.Contains(publishResult.MessageId))
            {
                ownershipConflictCount += 1;
                continue;
            }

            if (publishResult.IsConfirmed)
            {
                publishedCount += 1;
                continue;
            }

            failedCount += 1;

            logger.LogError(
                publishResult.Failure,
                "Failed to publish outbox message {MessageId}.",
                publishResult.MessageId);
        }

        OutboxMetrics.Published.Add(publishedCount);
        OutboxMetrics.Failed.Add(failedCount);

        if (ownershipConflictCount > 0)
        {
            OutboxMetrics.OwnershipConflicts.Add(ownershipConflictCount);

            logger.LogWarning(
                "Could not finalize {Count} outbox messages because " +
                "their lock ownership changed.",
                ownershipConflictCount);
        }
    }

    private sealed record ClaimedBatch(
        string LockOwner,
        IReadOnlyList<OutboxPublishRequest> Messages);
}
