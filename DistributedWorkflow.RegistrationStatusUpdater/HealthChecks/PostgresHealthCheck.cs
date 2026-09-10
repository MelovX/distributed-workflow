using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;

namespace DistributedWorkflow.RegistrationStatusUpdater.HealthChecks
{
    public sealed class PostgresHealthCheck(
        NpgsqlDataSource dataSource) : IHealthCheck
    {
        public async Task<HealthCheckResult> CheckHealthAsync(
            HealthCheckContext context,
            CancellationToken cancellationToken = default)
        {
            try
            {
                await using var connection =
                    await dataSource.OpenConnectionAsync(cancellationToken);

                await using var command =
                    new NpgsqlCommand("SELECT 1;", connection);

                var result =
                    await command.ExecuteScalarAsync(cancellationToken);

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
