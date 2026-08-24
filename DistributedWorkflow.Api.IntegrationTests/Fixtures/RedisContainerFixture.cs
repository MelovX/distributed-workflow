using Testcontainers.Redis;

namespace DistributedWorkflow.Api.IntegrationTests.Fixtures
{
    public sealed class RedisContainerFixture : IAsyncLifetime
    {
        public RedisContainer Container { get; } = new RedisBuilder("redis:7").Build();

        public async ValueTask InitializeAsync()
        {
            await Container.StartAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await Container.DisposeAsync();
        }
    }
}
