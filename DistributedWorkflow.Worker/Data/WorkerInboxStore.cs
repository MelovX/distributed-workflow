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

        public async Task<InboxClaimStatus> ClaimAsync(
            Guid messageId,
            string operationId,
            string messageType,
            string lockOwner,
            CancellationToken cancellationToken)
        {
            const string claimSql =
                """
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
                VALUES
                (
                    @messageId,
                    @operationId,
                    @messageType,
                    clock_timestamp(),
                    NULL,
                    1,
                    clock_timestamp() + @leaseDuration,
                    @lockOwner,
                    NULL
                )
                ON CONFLICT ("MessageId") DO UPDATE
                SET
                    "Attempts" = inbox."Attempts" + 1,
                    "LockedUntil" =
                        clock_timestamp() + @leaseDuration,
                    "LockedBy" = @lockOwner,
                    "LastError" = NULL
                WHERE inbox."CompletedAt" IS NULL
                  AND (
                      inbox."LockedUntil" IS NULL
                      OR inbox."LockedUntil" <= clock_timestamp()
                  )
                RETURNING TRUE;
                """;

            await using var connection =
                await _dataSource.OpenConnectionAsync(cancellationToken);

            await using var claimCommand =
                new NpgsqlCommand(claimSql, connection);

            claimCommand.Parameters.AddWithValue(
                "messageId",
                NpgsqlDbType.Uuid,
                messageId);
            claimCommand.Parameters.AddWithValue(
                "operationId",
                NpgsqlDbType.Text,
                operationId);
            claimCommand.Parameters.AddWithValue(
                "messageType",
                NpgsqlDbType.Text,
                messageType);
            claimCommand.Parameters.AddWithValue(
                "leaseDuration",
                NpgsqlDbType.Interval,
                LeaseDuration);
            claimCommand.Parameters.AddWithValue(
                "lockOwner",
                NpgsqlDbType.Text,
                lockOwner);

            var acquired =
                await claimCommand.ExecuteScalarAsync(cancellationToken);

            if (acquired is true)
            {
                return InboxClaimStatus.Acquired;
            }

            const string stateSql =
                """
                SELECT "CompletedAt" IS NOT NULL
                FROM "WorkerInboxMessages"
                WHERE "MessageId" = @messageId;
                """;

            await using var stateCommand =
                new NpgsqlCommand(stateSql, connection);

            stateCommand.Parameters.AddWithValue(
                "messageId",
                NpgsqlDbType.Uuid,
                messageId);

            var completed =
                await stateCommand.ExecuteScalarAsync(cancellationToken);

            return completed switch
            {
                true => InboxClaimStatus.AlreadyCompleted,
                false => InboxClaimStatus.LeaseHeld,
                _ => throw new InvalidOperationException(
                    "Inbox message disappeared after claim conflict.")
            };
        }

        public async Task<bool> CompleteAndEnqueueAsync(
            Guid messageId,
            string lockOwner,
            KafkaOutboxEntry outboxEntry,
            CancellationToken cancellationToken)
        {
            const string sql =
                """
                WITH completed AS
                (
                    UPDATE "WorkerInboxMessages"
                    SET
                        "CompletedAt" = clock_timestamp(),
                        "LockedUntil" = NULL,
                        "LockedBy" = NULL,
                        "LastError" = NULL
                    WHERE "MessageId" = @messageId
                      AND "CompletedAt" IS NULL
                      AND "LockedBy" = @lockOwner
                    RETURNING TRUE
                )
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
                    @outboxMessageId,
                    @operationId,
                    @messageType,
                    @partitionKey,
                    @payload,
                    clock_timestamp(),
                    NULL,
                    0,
                    NULL,
                    NULL,
                    NULL
                FROM completed
                RETURNING TRUE;
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
                "outboxMessageId",
                NpgsqlDbType.Uuid,
                outboxEntry.Id);
            command.Parameters.AddWithValue(
                "operationId",
                NpgsqlDbType.Text,
                outboxEntry.OperationId);
            command.Parameters.AddWithValue(
                "messageType",
                NpgsqlDbType.Text,
                outboxEntry.MessageType);
            command.Parameters.AddWithValue(
                "partitionKey",
                NpgsqlDbType.Text,
                outboxEntry.PartitionKey);
            command.Parameters.AddWithValue(
                "payload",
                NpgsqlDbType.Text,
                outboxEntry.Payload);

            var completed =
                await command.ExecuteScalarAsync(cancellationToken);

            return completed is true;
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
