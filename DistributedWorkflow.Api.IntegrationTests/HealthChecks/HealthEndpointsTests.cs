using DistributedWorkflow.Api.IntegrationTests.Factories;
using System.Net;

namespace DistributedWorkflow.Api.IntegrationTests.HealthChecks
{
    public sealed class HealthEndpointsTests : IClassFixture<ApiWebApplicationFactory>
    {
        private readonly ApiWebApplicationFactory _factory;

        public HealthEndpointsTests(ApiWebApplicationFactory factory)
        {
            _factory = factory;
        }

        [Fact]
        public async Task Live_WhenApplicationIsRunning_ReturnsHealthy()
        {
            // Arrange
            using var client = _factory.CreateClient();

            // Act
            using var response = await client.GetAsync(
                "/health/live",
                TestContext.Current.CancellationToken);

            var content = await response.Content.ReadAsStringAsync(
                TestContext.Current.CancellationToken);

            // Assert
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("Healthy", content);
        }
    }
}
