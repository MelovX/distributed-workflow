using Interview.Playground.Api.Data;
using Microsoft.EntityFrameworkCore;
using Testcontainers.PostgreSql;

namespace Interview.Playground.Api.IntegrationTests.Fixtures
{
    public sealed class PostgresContainerFixture : IAsyncLifetime
    {
        public PostgreSqlContainer Container { get; } =
            new PostgreSqlBuilder("postgres:16")
                .WithDatabase("registration_db")
                .WithUsername("postgres")
                .WithPassword("postgres")
                .Build();

        public async ValueTask InitializeAsync()
        {
            await Container.StartAsync();

            var options = new DbContextOptionsBuilder<RegistrationDbContext>()
                .UseNpgsql(Container.GetConnectionString())
                .Options;

            await using var dbContext = new RegistrationDbContext(options);

            await dbContext.Database.MigrateAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await Container.DisposeAsync();
        }
    }
}
