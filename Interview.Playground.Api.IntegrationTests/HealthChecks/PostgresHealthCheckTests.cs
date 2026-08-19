using Interview.Playground.Api.Data;
using Interview.Playground.Api.HealthChecks;
using Interview.Playground.Api.IntegrationTests.Fixtures;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;

namespace Interview.Playground.Api.IntegrationTests.HealthChecks
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
            var services = new ServiceCollection();

            services.AddDbContext<RegistrationDbContext>(options =>
                options.UseNpgsql(_fixture.Container.GetConnectionString()));

            await using var serviceProvider = services.BuildServiceProvider();

            var scopeFactory =
                serviceProvider.GetRequiredService<IServiceScopeFactory>();

            var healthCheck = new PostgresHealthCheck(scopeFactory);
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

            var services = new ServiceCollection();

            services.AddDbContext<RegistrationDbContext>(options =>
                options.UseNpgsql(connectionStringBuilder.ConnectionString));

            await using var serviceProvider = services.BuildServiceProvider();

            var scopeFactory =
                serviceProvider.GetRequiredService<IServiceScopeFactory>();

            var healthCheck = new PostgresHealthCheck(scopeFactory);
            var context = new HealthCheckContext();

            // Act
            var result = await healthCheck.CheckHealthAsync(
                context,
                TestContext.Current.CancellationToken);

            // Assert
            Assert.Equal(HealthStatus.Unhealthy, result.Status);
            Assert.Equal("Can't connect to PostgreSQL.", result.Description);
        }
    }
}
