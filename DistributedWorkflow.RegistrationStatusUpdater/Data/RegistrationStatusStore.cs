using Npgsql;
using NpgsqlTypes;

namespace DistributedWorkflow.RegistrationStatusUpdater.Data
{
    public sealed class RegistrationStatusStore(
        NpgsqlDataSource dataSource)
    {
        public async Task<int> MarkSucceededAsync(
            IReadOnlyCollection<string> operationIds,
            CancellationToken cancellationToken)
        {
            if (operationIds.Count == 0)
            {
                return 0;
            }

            const string sql =
                """
                UPDATE "RegistrationOperations" AS operation
                SET "Status" = 'Succeeded'
                FROM unnest(@operationIds::text[]) AS events("OperationId")
                WHERE operation."Id" = events."OperationId"
                  AND operation."Status" = 'Pending';
                """;

            await using var connection =
                await dataSource.OpenConnectionAsync(cancellationToken);

            await using var command =
                new NpgsqlCommand(sql, connection);

            command.Parameters.AddWithValue(
                "operationIds",
                NpgsqlDbType.Array | NpgsqlDbType.Text,
                operationIds.Distinct().ToArray());

            return await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }
}
