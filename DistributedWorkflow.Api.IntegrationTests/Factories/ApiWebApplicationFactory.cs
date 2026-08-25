using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace DistributedWorkflow.Api.IntegrationTests.Factories
{
    public sealed class ApiWebApplicationFactory
        : WebApplicationFactory<Program>
    {
        private readonly string _postgresConnectionString;
        private readonly string _redisConnectionString;

        public ApiWebApplicationFactory()
            : this("Host=localhost;Port=1;Database=test;Username=test;Password=test", "localhost:1")
        { }

        internal ApiWebApplicationFactory(
            string postgresConnectionString,
            string redisConnectionString)
        {
            _postgresConnectionString = postgresConnectionString;
            _redisConnectionString = redisConnectionString;
        }

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");

            builder.UseSetting(
                "ConnectionStrings:RegistrationDb",
                _postgresConnectionString);

            builder.UseSetting(
                "ConnectionStrings:Redis",
                _redisConnectionString);

            builder.ConfigureTestServices(services =>
            {
                services.RemoveAll<IHostedService>();
            });
        }
    }
}
