using DistributedWorkflow.Api.HealthChecks;
using DistributedWorkflow.Api.IntegrationTests.Fixtures;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;

namespace DistributedWorkflow.Api.IntegrationTests.HealthChecks
{
    public sealed class PostgresHealthCheckTests : IClassFixture<PostgresContainerFixture>
    {
        private readonly PostgresContainerFixture _fixture;

        public PostgresHealthCheckTests(PostgresContainerFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task CheckHealthAsync_WhenPostgresIsAvailable_ReturnsHealthy()
        {
            // Arrange
            var dataSource =
                NpgsqlDataSource.Create(_fixture.Container.GetConnectionString());

            var healthCheck = new PostgresHealthCheck(dataSource);
            var context = new HealthCheckContext();

            // Act
            var result = await healthCheck.CheckHealthAsync(
                context,
                TestContext.Current.CancellationToken);

            // Assert
            Assert.Equal(HealthStatus.Healthy, result.Status);
        }

        [Fact]
        public async Task CheckHealthAsync_WhenDatabaseDoesNotExist_ReturnsUnhealthy()
        {
            // Arrange
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(
                _fixture.Container.GetConnectionString())
            {
                Database = "missing_database"
            };

            var dataSource =
                NpgsqlDataSource.Create(connectionStringBuilder.ConnectionString);

            var healthCheck = new PostgresHealthCheck(dataSource);
            var context = new HealthCheckContext();

            // Act
            var result = await healthCheck.CheckHealthAsync(
                context,
                TestContext.Current.CancellationToken);

            // Assert
            Assert.Equal(HealthStatus.Unhealthy, result.Status);
        }
    }
}
