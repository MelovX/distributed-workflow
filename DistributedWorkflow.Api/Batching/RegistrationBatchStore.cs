using DistributedWorkflow.Api.Metrics;
using Npgsql;
using NpgsqlTypes;

namespace DistributedWorkflow.Api.Batching
{
    internal sealed class RegistrationBatchStore : IRegistrationBatchStore
    {
        private readonly NpgsqlDataSource _dataSource;

        public RegistrationBatchStore(NpgsqlDataSource dataSource)
        {
            _dataSource = dataSource;
        }

        public async Task<IReadOnlyList<RegistrationWriteResult>> WriteAsync(
            IReadOnlyList<RegistrationWriteRequest> requests,
            CancellationToken cancellationToken)
        {
            if (requests.Count == 0)
            {
                return Array.Empty<RegistrationWriteResult>();
            }

            await using var connection =
                await _dataSource.OpenConnectionAsync(cancellationToken);

            await using var transaction =
                await connection.BeginTransactionAsync(
                    cancellationToken);

            var insertedCount = await InsertNewRegistrationsAsync(
                connection,
                transaction,
                requests,
                cancellationToken);

            var results = await ReadResultsAsync(
                connection,
                transaction,
                requests,
                cancellationToken);

            if (results.Count != requests.Count)
            {
                throw new InvalidOperationException(
                    "Not all registration batch results were found.");
            }

            await transaction.CommitAsync(cancellationToken);

            RegistrationMetrics.RegistrationsCreated.Add(
                insertedCount);

            return results;
        }

        private static async Task<int> InsertNewRegistrationsAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            IReadOnlyList<RegistrationWriteRequest> requests,
            CancellationToken cancellationToken)
        {
            const string sql =
                """
                WITH input AS
                (
                    SELECT *
                    FROM unnest (
                        @operationIds::text[],
                        @idempotencyKeys::text[],
                        @documentIds::text[],
                        @titles::text[],
                        @statuses::text[],
                        @operationCreatedAt::timestamptz[],
                        @outboxMessageIds::uuid[],
                        @outboxMessageTypes::text[],
                        @outboxPayloads::text[],
                        @outboxCreatedAt::timestamptz[]
                    ) AS input (
                        "OperationId",
                        "IdempotencyKey",
                        "DocumentId",
                        "Title",
                        "Status",
                        "OperationCreatedAt",
                        "OutboxMessageId",
                        "OutboxMessageType",
                        "OutboxPayload",
                        "OutboxCreatedAt"
                    )
                ),
                inserted_operations AS
                (
                    INSERT INTO "RegistrationOperations"
                    (
                        "Id",
                        "IdempotencyKey",
                        "DocumentId",
                        "Title",
                        "Status",
                        "CreatedAt"
                    )
                    SELECT
                        input."OperationId",
                        input."IdempotencyKey",
                        input."DocumentId",
                        input."Title",
                        input."Status",
                        input."OperationCreatedAt"
                    FROM input
                    ON CONFLICT ("IdempotencyKey") DO NOTHING
                    RETURNING "Id"
                )
                INSERT INTO "OutboxMessages"
                (
                    "Id",
                    "Type",
                    "Payload",
                    "CreatedAt"
                )
                SELECT
                    input."OutboxMessageId",
                    input."OutboxMessageType",
                    input."OutboxPayload",
                    input."OutboxCreatedAt"
                FROM input
                INNER JOIN inserted_operations
                    ON inserted_operations."Id" = input."OperationId";
                """;

            await using var command =
                new NpgsqlCommand(sql, connection, transaction);

            command.Parameters.AddWithValue(
                "operationIds",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                requests.Select(request => request.Operation.Id).ToArray());

            command.Parameters.AddWithValue(
                "idempotencyKeys",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                requests.Select(request => request.Operation.IdempotencyKey).ToArray());

            command.Parameters.AddWithValue(
                "documentIds",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                requests.Select(request => request.Operation.DocumentId).ToArray());

            command.Parameters.AddWithValue(
                "titles",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                requests.Select(request => request.Operation.Title).ToArray());

            command.Parameters.AddWithValue(
                "statuses",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                requests.Select(request => request.Operation.Status).ToArray());

            command.Parameters.AddWithValue(
                "operationCreatedAt",
                NpgsqlDbType.Array | NpgsqlDbType.TimestampTz,
                requests.Select(request => request.Operation.CreatedAt).ToArray());

            command.Parameters.AddWithValue(
                "outboxMessageIds",
                NpgsqlDbType.Array | NpgsqlDbType.Uuid,
                requests.Select(request => request.OutboxMessage.Id).ToArray());

            command.Parameters.AddWithValue(
                "outboxMessageTypes",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                requests.Select(request => request.OutboxMessage.Type).ToArray());

            command.Parameters.AddWithValue(
                "outboxPayloads",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                requests.Select(request => request.OutboxMessage.Payload).ToArray());

            command.Parameters.AddWithValue(
                "outboxCreatedAt",
                NpgsqlDbType.Array | NpgsqlDbType.TimestampTz,
                requests.Select(request => request.OutboxMessage.CreatedAt).ToArray());

            return await command.ExecuteNonQueryAsync(cancellationToken);
        }

        private static async Task<IReadOnlyList<RegistrationWriteResult>> ReadResultsAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            IReadOnlyList<RegistrationWriteRequest> requests,
            CancellationToken cancellationToken)
        {
            const string sql =
                """
                SELECT
                    operations."Id",
                    operations."Status"
                FROM unnest(
                    @idempotencyKeys::text[]
                ) WITH ORDINALITY AS input (
                    "IdempotencyKey",
                    "Position"
                )
                INNER JOIN "RegistrationOperations" AS operations
                    ON operations."IdempotencyKey" =
                       input."IdempotencyKey"
                ORDER BY input."Position";
                """;

            await using var command =
                new NpgsqlCommand(sql, connection, transaction);

            command.Parameters.AddWithValue(
                "idempotencyKeys",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                requests
                    .Select(request =>
                        request.Operation.IdempotencyKey)
                    .ToArray());

            var results =
                new List<RegistrationWriteResult>(requests.Count);

            await using var reader =
                await command.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                results.Add(
                    new RegistrationWriteResult(
                        reader.GetString(0),
                        reader.GetString(1)));
            }

            return results;
        }
    }
}
