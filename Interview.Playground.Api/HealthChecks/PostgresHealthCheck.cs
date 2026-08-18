using Interview.Playground.Api.Data;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Interview.Playground.Api.HealthChecks
{
    public class PostgresHealthCheck : IHealthCheck
    {
        private readonly IServiceScopeFactory _scopeFactory;
        public PostgresHealthCheck(IServiceScopeFactory scopeFactory)
        {
            _scopeFactory = scopeFactory;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            await using var scope = _scopeFactory.CreateAsyncScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<RegistrationDbContext>();

            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                var canConnect = await dbContext.Database.CanConnectAsync(cancellationToken);

                if (canConnect)
                {
                    return HealthCheckResult.Healthy();
                }
                return HealthCheckResult.Unhealthy("Can't connect to PostgreSQL.");
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                return HealthCheckResult.Unhealthy(ex.InnerException?.Message, ex);
            }
        }
    }
}
