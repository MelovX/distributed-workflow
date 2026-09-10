using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

namespace DistributedWorkflow.Api.IntegrationTests.Factories
{
    public sealed class ApiWebApplicationFactory
        : WebApplicationFactory<Program>
    {
        private readonly string _postgresConnectionString;

        public ApiWebApplicationFactory()
            : this("Host=localhost;Port=1;Database=test;Username=test;Password=test")
        { }

        internal ApiWebApplicationFactory(
            string postgresConnectionString)
        {
            _postgresConnectionString = postgresConnectionString;
        }

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");

            builder.UseSetting(
                "ConnectionStrings:RegistrationDb",
                _postgresConnectionString);
        }
    }
}
