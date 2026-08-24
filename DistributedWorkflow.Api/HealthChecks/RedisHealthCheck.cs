using Microsoft.Extensions.Diagnostics.HealthChecks;
using StackExchange.Redis;

namespace DistributedWorkflow.Api.HealthChecks
{
    public class RedisHealthCheck : IHealthCheck
    {
        private readonly IConnectionMultiplexer _redis;

        public RedisHealthCheck(IConnectionMultiplexer redis)
        {
            _redis = redis;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(
            HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            var db = _redis.GetDatabase();

            try
            {
                await db.PingAsync().WaitAsync(cancellationToken);

                return HealthCheckResult.Healthy();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                return HealthCheckResult.Unhealthy(ex.Message, ex);
            }
        }
    }
}
