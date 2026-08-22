using Interview.Playground.Api.IntegrationTests.Factories;
using Interview.Playground.Api.IntegrationTests.Fixtures;
using System.Net;

namespace Interview.Playground.Api.IntegrationTests.HealthChecks
{
    public sealed class ReadyHealthEndpointsTests 
        : IClassFixture<PostgresContainerFixture>, 
          IClassFixture<RedisContainerFixture>
    {
        private readonly PostgresContainerFixture _postgresFixture;
        private readonly RedisContainerFixture _redisFixture;

        public ReadyHealthEndpointsTests(PostgresContainerFixture postgresFixture,
            RedisContainerFixture redisContainerFixture)
        {
            _postgresFixture = postgresFixture;
            _redisFixture = redisContainerFixture;
        }

        [Fact]
        public async Task Ready_WhenDependenciesAreAvailable_ReturnsHealthy()
        {
            // Arrange
            using var factory = new ApiWebApplicationFactory(
                _postgresFixture.Container.GetConnectionString(),
                _redisFixture.Container.GetConnectionString());

            using var client = factory.CreateClient();

            // Act
            using var response = await client.GetAsync(
                "/health/ready",
                TestContext.Current.CancellationToken);

            var content = await response.Content.ReadAsStringAsync(
                TestContext.Current.CancellationToken);

            // Assert
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("Healthy", content);
        }
    }
}
