using Interview.Playground.Api.HealthChecks;
using Interview.Playground.Api.IntegrationTests.Fixtures;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using StackExchange.Redis;

namespace Interview.Playground.Api.IntegrationTests.HealthChecks
{
    public sealed class RedisHealthCheckTests : IClassFixture<RedisContainerFixture>
    {
        private readonly RedisContainerFixture _fixture;

        public RedisHealthCheckTests(RedisContainerFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task CheckHealthAsync_WhenRedisIsAvailable_ReturnsHealthy()
        {
            // Arrange
            await using var connectionMultiplexer = await ConnectionMultiplexer
                .ConnectAsync(_fixture.Container.GetConnectionString());

            var healthCheck = new RedisHealthCheck(connectionMultiplexer);
            var context = new HealthCheckContext();

            // Act
            var result = await healthCheck.CheckHealthAsync(
                context,
                TestContext.Current.CancellationToken);

            // Assert
            Assert.Equal(HealthStatus.Healthy, result.Status);
        }
    }
}
