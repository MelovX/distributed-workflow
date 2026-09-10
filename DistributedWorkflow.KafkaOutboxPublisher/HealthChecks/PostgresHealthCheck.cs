using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;

namespace DistributedWorkflow.KafkaOutboxPublisher.HealthChecks
{
    public sealed class PostgresHealthCheck : IHealthCheck
    {
        private readonly NpgsqlDataSource _dataSource;

        public PostgresHealthCheck(NpgsqlDataSource dataSource)
        {
            _dataSource = dataSource;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(
            HealthCheckContext context,
            CancellationToken cancellationToken = default)
        {
            try
            {
                await using var connection =
                    await _dataSource.OpenConnectionAsync(
                        cancellationToken);

                await using var command =
                    new NpgsqlCommand("SELECT 1;", connection);

                var result =
                    await command.ExecuteScalarAsync(
                        cancellationToken);

                return result is not null
                    ? HealthCheckResult.Healthy()
                    : HealthCheckResult.Unhealthy(
                        "PostgreSQL health query returned no result.");
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                return HealthCheckResult.Unhealthy(
                    exception.Message,
                    exception);
            }
        }
    }
}
