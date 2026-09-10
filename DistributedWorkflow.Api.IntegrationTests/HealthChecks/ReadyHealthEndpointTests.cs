using DistributedWorkflow.Api.IntegrationTests.Factories;
using DistributedWorkflow.Api.IntegrationTests.Fixtures;
using Npgsql;
using System.Net;

namespace DistributedWorkflow.Api.IntegrationTests.HealthChecks
{
    public sealed class ReadyHealthEndpointsTests 
        : IClassFixture<PostgresContainerFixture>
    {
        private readonly PostgresContainerFixture _postgresFixture;

        public ReadyHealthEndpointsTests(PostgresContainerFixture postgresFixture)
        {
            _postgresFixture = postgresFixture;
        }

        [Fact]
        public async Task Ready_WhenDependenciesAreAvailable_ReturnsHealthy()
        {
            // Arrange
            using var factory = new ApiWebApplicationFactory(
                _postgresFixture.Container.GetConnectionString());

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

        [Fact]
        public async Task Ready_WhenPostgresDatabaseDoesNotExist_ReturnsServiceUnavailable()
        {
            // Arrange
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(
                _postgresFixture.Container.GetConnectionString())
            {
                Database = "missing_database"
            };

            using var factory = new ApiWebApplicationFactory(
                connectionStringBuilder.ConnectionString);

            using var client = factory.CreateClient();

            // Act
            using var response = await client.GetAsync(
                "/health/ready",
                TestContext.Current.CancellationToken);

            var content = await response.Content.ReadAsStringAsync(
                TestContext.Current.CancellationToken);

            // Assert
            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
            Assert.Equal("Unhealthy", content);
        }
    }
}
