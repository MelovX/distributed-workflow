using DistributedWorkflow.KafkaOutboxPublisher.Configuration;
using DistributedWorkflow.KafkaOutboxPublisher.Entities;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace DistributedWorkflow.KafkaOutboxPublisher.Data
{
    public sealed class KafkaOutboxStore
    {
        private readonly NpgsqlDataSource _dataSource;
        private readonly int _batchSize;
        private readonly TimeSpan _leaseDuration;

        private readonly string _publisherId =
            $"{Environment.MachineName}-{Guid.NewGuid():N}";

        public KafkaOutboxStore(
            NpgsqlDataSource dataSource,
            IOptions<OutboxOptions> options)
        {
            _dataSource = dataSource;
            _batchSize = options.Value.BatchSize;
            _leaseDuration = options.Value.LeaseDuration;
        }

        public async Task<ClaimedOutboxBatch> ClaimBatchAsync(
            CancellationToken cancellationToken)
        {
            var lockOwner =
                $"{_publisherId}-{Guid.NewGuid():N}";

            const string sql =
                """
                WITH candidates AS
                (
                    SELECT "Id"
                    FROM "WorkerKafkaOutboxMessages"
                    WHERE "PublishedAt" IS NULL
                      AND (
                          "LockedUntil" IS NULL
                          OR "LockedUntil" <= clock_timestamp()
                      )
                    ORDER BY "CreatedAt", "Id"
                    LIMIT @batchSize
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE "WorkerKafkaOutboxMessages" AS messages
                SET
                    "LockedUntil" =
                        clock_timestamp() + @leaseDuration,
                    "LockedBy" = @lockOwner
                FROM candidates
                WHERE messages."Id" = candidates."Id"
                RETURNING
                    messages."Id",
                    messages."OperationId",
                    messages."MessageType",
                    messages."PartitionKey",
                    messages."Payload";
                """;

            await using var connection =
                await _dataSource.OpenConnectionAsync(
                    cancellationToken);

            await using var command =
                new NpgsqlCommand(sql, connection);

            command.Parameters.AddWithValue(
                "batchSize",
                NpgsqlDbType.Integer,
                _batchSize);
            command.Parameters.AddWithValue(
                "leaseDuration",
                NpgsqlDbType.Interval,
                _leaseDuration);
            command.Parameters.AddWithValue(
                "lockOwner",
                NpgsqlDbType.Text,
                lockOwner);

            var messages =
                new List<KafkaOutboxPublishRequest>(_batchSize);

            await using var reader =
                await command.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                messages.Add(
                    new KafkaOutboxPublishRequest(
                        reader.GetGuid(0),
                        reader.GetString(1),
                        reader.GetString(2),
                        reader.GetString(3),
                        reader.GetString(4)));
            }

            return new ClaimedOutboxBatch(
                lockOwner,
                messages);
        }

        public async Task<OutboxBatchCompletion> CompleteBatchAsync(
            string lockOwner,
            IReadOnlyList<KafkaOutboxPublishResult> results,
            CancellationToken cancellationToken)
        {
            if (results.Count == 0)
            {
                return new OutboxBatchCompletion(0, 0, 0);
            }

            var messageIds = new Guid[results.Count];
            var published = new bool[results.Count];
            var failureMessages = new string[results.Count];

            for (var index = 0; index < results.Count; index++)
            {
                var result = results[index];

                messageIds[index] = result.MessageId;
                published[index] = result.IsPublished;
                failureMessages[index] =
                    result.Failure?.Message
                    ?? "Kafka did not confirm the message.";
            }

            const string sql =
                """
                UPDATE "WorkerKafkaOutboxMessages" AS messages
                SET
                    "PublishedAt" =
                        CASE
                            WHEN results."IsPublished"
                                THEN statement_timestamp()
                            ELSE messages."PublishedAt"
                        END,
                    "LockedUntil" =
                        CASE
                            WHEN results."IsPublished"
                                THEN NULL
                            ELSE messages."LockedUntil"
                        END,
                    "LockedBy" =
                        CASE
                            WHEN results."IsPublished"
                                THEN NULL
                            ELSE messages."LockedBy"
                        END,
                    "Attempts" =
                        CASE
                            WHEN results."IsPublished"
                                THEN messages."Attempts"
                            ELSE messages."Attempts" + 1
                        END,
                    "LastError" =
                        CASE
                            WHEN results."IsPublished"
                                THEN NULL
                            ELSE results."FailureMessage"
                        END
                FROM unnest(
                    @messageIds::uuid[],
                    @published::boolean[],
                    @failureMessages::text[])
                    AS results(
                        "MessageId",
                        "IsPublished",
                        "FailureMessage")
                WHERE messages."Id" = results."MessageId"
                  AND messages."PublishedAt" IS NULL
                  AND messages."LockedBy" = @lockOwner
                RETURNING results."IsPublished";
                """;

            await using var connection =
                await _dataSource.OpenConnectionAsync(cancellationToken);

            await using var command =
                new NpgsqlCommand(sql, connection);

            command.Parameters.AddWithValue(
                "messageIds",
                NpgsqlDbType.Array | NpgsqlDbType.Uuid,
                messageIds);
            command.Parameters.AddWithValue(
                "published",
                NpgsqlDbType.Array | NpgsqlDbType.Boolean,
                published);
            command.Parameters.AddWithValue(
                "failureMessages",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                failureMessages);
            command.Parameters.AddWithValue(
                "lockOwner",
                NpgsqlDbType.Text,
                lockOwner);

            var publishedCount = 0;
            var failedCount = 0;
            var updatedCount = 0;

            await using var reader =
                await command.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                updatedCount++;

                if (reader.GetBoolean(0))
                {
                    publishedCount++;
                }
                else
                {
                    failedCount++;
                }
            }

            return new OutboxBatchCompletion(
                publishedCount,
                failedCount,
                results.Count - updatedCount);
        }
    }
}