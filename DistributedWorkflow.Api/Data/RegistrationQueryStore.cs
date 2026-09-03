using Npgsql;
using NpgsqlTypes;

namespace DistributedWorkflow.Api.Data
{
    public sealed class RegistrationQueryStore(
        NpgsqlDataSource dataSource)
    {
        public async Task<(string OperationId, string Status)?> FindByOperationIdAsync(
            string operationId, CancellationToken cancellationToken)
        {
            const string sql =
                """
                SELECT
                    "Id",
                    "Status"
                FROM "RegistrationOperations"
                WHERE "Id" = @operationId;
                """;

            await using var connection =
                await dataSource.OpenConnectionAsync(cancellationToken);

            await using var command =
                new NpgsqlCommand(sql, connection);

            command.Parameters.AddWithValue(
                "operationId",
                NpgsqlDbType.Text,
                operationId);

            await using var reader =
                await command.ExecuteReaderAsync(cancellationToken);

            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return (
                reader.GetString(0),
                reader.GetString(1));
        }
    }
}