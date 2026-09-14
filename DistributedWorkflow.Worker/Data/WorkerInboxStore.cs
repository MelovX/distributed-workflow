using Npgsql;
using NpgsqlTypes;

namespace DistributedWorkflow.Worker.Data
{
    public sealed class WorkerInboxStore
    {
        private readonly NpgsqlDataSource _dataSource;
        private static readonly TimeSpan LeaseDuration =
            TimeSpan.FromSeconds(30);

        public WorkerInboxStore(NpgsqlDataSource dataSource)
        {
            _dataSource = dataSource;
        }

        public async Task<IReadOnlyList<InboxClaimResult>> ClaimBatchAsync(
            IReadOnlyList<InboxClaimEntry> entries,
            CancellationToken cancellationToken)
        {
            if (entries.Count == 0)
            {
                return Array.Empty<InboxClaimResult>();
            }

            const string sql =
                """
                WITH input AS
                (
                    SELECT *
                    FROM unnest(
                        @messageIds::uuid[],
                        @operationIds::text[],
                        @messageTypes::text[],
                        @lockOwners::text[]
                    ) WITH ORDINALITY AS input(
                        "MessageId",
                        "OperationId",
                        "MessageType",
                        "LockOwner",
                        "Position"
                    )
                ),
                unique_input AS
                (
                    SELECT DISTINCT ON ("MessageId")
                        "MessageId",
                        "OperationId",
                        "MessageType",
                        "LockOwner",
                        "Position"
                    FROM input
                    ORDER BY
                        "MessageId",
                        "Position"
                ),
                claimed AS
                (
                    INSERT INTO "WorkerInboxMessages" AS inbox
                    (
                        "MessageId",
                        "OperationId",
                        "MessageType",
                        "ReceivedAt",
                        "CompletedAt",
                        "Attempts",
                        "LockedUntil",
                        "LockedBy",
                        "LastError"
                    )
                    SELECT
                        unique_input."MessageId",
                        unique_input."OperationId",
                        unique_input."MessageType",
                        clock_timestamp(),
                        NULL,
                        1,
                        clock_timestamp() + @leaseDuration,
                        unique_input."LockOwner",
                        NULL
                    FROM unique_input
                    ON CONFLICT ("MessageId") DO UPDATE
                    SET
                        "Attempts" = inbox."Attempts" + 1,
                        "LockedUntil" =
                            clock_timestamp() + @leaseDuration,
                        "LockedBy" = EXCLUDED."LockedBy",
                        "LastError" = NULL
                    WHERE inbox."CompletedAt" IS NULL
                      AND (
                          inbox."LockedUntil" IS NULL
                          OR inbox."LockedUntil" <= clock_timestamp()
                      )
                    RETURNING
                        "MessageId",
                        "LockedBy"
                )
                SELECT
                    input."MessageId",
                    input."LockOwner",
                    CASE
                        WHEN claimed."LockedBy" = input."LockOwner"
                            THEN 0
                        WHEN inbox."CompletedAt" IS NOT NULL
                            THEN 1
                        ELSE 2
                    END AS "Status"
                FROM input
                LEFT JOIN claimed
                    ON claimed."MessageId" = input."MessageId"
                LEFT JOIN "WorkerInboxMessages" AS inbox
                    ON inbox."MessageId" = input."MessageId"
                ORDER BY input."Position";
                """;

            await using var connection =
                await _dataSource.OpenConnectionAsync(cancellationToken);

            await using var command =
                new NpgsqlCommand(sql, connection);

            command.Parameters.AddWithValue(
                "messageIds",
                NpgsqlDbType.Array | NpgsqlDbType.Uuid,
                entries
                    .Select(entry => entry.MessageId)
                    .ToArray());

            command.Parameters.AddWithValue(
                "operationIds",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                entries
                    .Select(entry => entry.OperationId)
                    .ToArray());

            command.Parameters.AddWithValue(
                "messageTypes",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                entries
                    .Select(entry => entry.MessageType)
                    .ToArray());

            command.Parameters.AddWithValue(
                "lockOwners",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                entries
                    .Select(entry => entry.LockOwner)
                    .ToArray());

            command.Parameters.AddWithValue(
                "leaseDuration",
                NpgsqlDbType.Interval,
                LeaseDuration);

            var results =
                new List<InboxClaimResult>(entries.Count);

            await using var reader =
                await command.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                var status = reader.GetInt32(2) switch
                {
                    0 => InboxClaimStatus.Acquired,
                    1 => InboxClaimStatus.AlreadyCompleted,
                    2 => InboxClaimStatus.LeaseHeld,
                    var value => throw new InvalidOperationException(
                        $"Unexpected inbox claim status value: {value}.")
                };

                results.Add(
                    new InboxClaimResult(
                        reader.GetGuid(0),
                        reader.GetString(1),
                        status));
            }

            if (results.Count != entries.Count)
            {
                throw new InvalidOperationException(
                    "Inbox batch claim returned an unexpected number of results.");
            }

            return results;
        }

        public async Task<IReadOnlyList<InboxCompletionResult>> CompleteAndEnqueueBatchAsync(
            IReadOnlyList<InboxCompletionEntry> entries,
            CancellationToken cancellationToken)
        {
            if (entries.Count == 0)
            {
                return Array.Empty<InboxCompletionResult>();
            }

            const string sql =
                """
                WITH input AS
                (
                    SELECT *
                    FROM unnest(
                        @messageIds::uuid[],
                        @lockOwners::text[],
                        @outboxMessageIds::uuid[],
                        @operationIds::text[],
                        @messageTypes::text[],
                        @partitionKeys::text[],
                        @payloads::text[]
                    ) WITH ORDINALITY AS input(
                        "MessageId",
                        "LockOwner",
                        "OutboxMessageId",
                        "OperationId",
                        "MessageType",
                        "PartitionKey",
                        "Payload",
                        "Position"
                    )
                ),
                unique_input AS
                (
                    SELECT DISTINCT ON ("MessageId")
                        "MessageId",
                        "LockOwner",
                        "OutboxMessageId",
                        "OperationId",
                        "MessageType",
                        "PartitionKey",
                        "Payload",
                        "Position"
                    FROM input
                    ORDER BY
                        "MessageId",
                        "Position"
                ),
                completed AS
                (
                    UPDATE "WorkerInboxMessages" AS inbox
                    SET
                        "CompletedAt" = clock_timestamp(),
                        "LockedUntil" = NULL,
                        "LockedBy" = NULL,
                        "LastError" = NULL
                    FROM unique_input
                    WHERE inbox."MessageId" = unique_input."MessageId"
                      AND inbox."CompletedAt" IS NULL
                      AND inbox."LockedBy" = unique_input."LockOwner"
                    RETURNING inbox."MessageId"
                ),
                inserted AS
                (
                    INSERT INTO "WorkerKafkaOutboxMessages"
                    (
                        "Id",
                        "OperationId",
                        "MessageType",
                        "PartitionKey",
                        "Payload",
                        "CreatedAt",
                        "PublishedAt",
                        "Attempts",
                        "LockedUntil",
                        "LockedBy",
                        "LastError"
                    )
                    SELECT
                        unique_input."OutboxMessageId",
                        unique_input."OperationId",
                        unique_input."MessageType",
                        unique_input."PartitionKey",
                        unique_input."Payload",
                        clock_timestamp(),
                        NULL,
                        0,
                        NULL,
                        NULL,
                        NULL
                    FROM unique_input
                    INNER JOIN completed
                        ON completed."MessageId" =
                           unique_input."MessageId"
                    RETURNING "Id"
                )
                SELECT
                    input."MessageId",
                    input."LockOwner",
                    (
                        completed."MessageId" IS NOT NULL
                        AND input."Position" =
                            unique_input."Position"
                    ) AS "Completed"
                FROM input
                INNER JOIN unique_input
                    ON unique_input."MessageId" =
                       input."MessageId"
                LEFT JOIN completed
                    ON completed."MessageId" =
                       input."MessageId"
                ORDER BY input."Position";
                """;

            await using var connection =
                await _dataSource.OpenConnectionAsync(cancellationToken);

            await using var command =
                new NpgsqlCommand(sql, connection);

            command.Parameters.AddWithValue(
                "messageIds",
                NpgsqlDbType.Array | NpgsqlDbType.Uuid,
                entries
                    .Select(entry => entry.MessageId)
                    .ToArray());

            command.Parameters.AddWithValue(
                "lockOwners",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                entries
                    .Select(entry => entry.LockOwner)
                    .ToArray());

            command.Parameters.AddWithValue(
                "outboxMessageIds",
                NpgsqlDbType.Array | NpgsqlDbType.Uuid,
                entries
                    .Select(entry => entry.OutboxEntry.Id)
                    .ToArray());

            command.Parameters.AddWithValue(
                "operationIds",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                entries
                    .Select(entry => entry.OutboxEntry.OperationId)
                    .ToArray());

            command.Parameters.AddWithValue(
                "messageTypes",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                entries
                    .Select(entry => entry.OutboxEntry.MessageType)
                    .ToArray());

            command.Parameters.AddWithValue(
                "partitionKeys",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                entries
                    .Select(entry => entry.OutboxEntry.PartitionKey)
                    .ToArray());

            command.Parameters.AddWithValue(
                "payloads",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                entries
                    .Select(entry => entry.OutboxEntry.Payload)
                    .ToArray());

            var results =
                new List<InboxCompletionResult>(entries.Count);

            await using var reader =
                await command.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                results.Add(
                    new InboxCompletionResult(
                        reader.GetGuid(0),
                        reader.GetString(1),
                        reader.GetBoolean(2)));
            }

            if (results.Count != entries.Count)
            {
                throw new InvalidOperationException(
                    "Inbox batch completion returned an unexpected number of results.");
            }

            return results;
        }

        public async Task<int?> FailAsync(
            Guid messageId,
            string lockOwner,
            string error,
            CancellationToken cancellationToken)
        {
            const string sql =
                """
                UPDATE "WorkerInboxMessages"
                SET
                    "LockedUntil" = NULL,
                    "LockedBy" = NULL,
                    "LastError" = @error
                WHERE "MessageId" = @messageId
                  AND "CompletedAt" IS NULL
                  AND "LockedBy" = @lockOwner
                RETURNING "Attempts";
                """;

            await using var connection =
                await _dataSource.OpenConnectionAsync(cancellationToken);

            await using var command =
                new NpgsqlCommand(sql, connection);

            command.Parameters.AddWithValue(
                "messageId",
                NpgsqlDbType.Uuid,
                messageId);

            command.Parameters.AddWithValue(
                "lockOwner",
                NpgsqlDbType.Text,
                lockOwner);

            command.Parameters.AddWithValue(
                "error",
                NpgsqlDbType.Text,
                error);

            var attempts =
                await command.ExecuteScalarAsync(cancellationToken);

            return (int?)attempts;
        }
    }
}
