using DistributedWorkflow.Api.Data;
using DistributedWorkflow.Api.HealthChecks;
using DistributedWorkflow.Api.Metrics;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using OpenTelemetry.Metrics;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var registrationDbConnectionString =
    builder.Configuration.GetConnectionString("RegistrationDb")
    ?? throw new InvalidOperationException(
        "Connection string 'RegistrationDb' is not configured.");

var runMigrations = string.Equals(
    builder.Configuration["RunMigrations"],
    "true",
    StringComparison.OrdinalIgnoreCase);

if (runMigrations)
{
    Console.WriteLine("Applying database migrations...");

    var dbContextOptions =
        new DbContextOptionsBuilder<RegistrationDbContext>()
            .UseNpgsql(registrationDbConnectionString)
            .Options;

    await using var dbContext = new RegistrationDbContext(dbContextOptions);

    await dbContext.Database.MigrateAsync();

    Console.WriteLine("Database migrations applied.");

    return;
}

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics
            .AddMeter(RegistrationMetrics.MeterName)
            .AddMeter("Npgsql")
            .AddView(
                "db.client.commands.duration",
                new ExplicitBucketHistogramConfiguration
                {
                    Boundaries =
                    [
                        0.001,
                        0.005,
                        0.010,
                        0.025,
                        0.050,
                        0.100,
                        0.250,
                        0.500,
                        1.000,
                        2.500,
                        5.000
                    ]
                })
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddRuntimeInstrumentation()
            .AddPrometheusExporter();
    });

var redisConnectionString =
    builder.Configuration.GetConnectionString("Redis")
    ?? throw new InvalidOperationException(
        "Connection string 'Redis' is not configured.");
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddControllers();
builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect(redisConnectionString));
builder.Services.AddDbContext<RegistrationDbContext>(options =>
{
    options.UseNpgsql(registrationDbConnectionString);
});
builder.Services.AddHealthChecks()
    .AddCheck<PostgresHealthCheck>("postgres",
        failureStatus: HealthStatus.Unhealthy,
        tags: new[] { "ready" })
    .AddCheck<RedisHealthCheck>(
        "redis",
        failureStatus: HealthStatus.Unhealthy,
        tags: new[] { "ready" });

var app = builder.Build();

app.MapHealthChecks("/health/ready", new HealthCheckOptions()
{
    Predicate = healthCheck => healthCheck.Tags.Contains("ready")
});
app.MapHealthChecks("/health/live", new HealthCheckOptions()
{
    Predicate = _ => false
});
app.UseOpenTelemetryPrometheusScrapingEndpoint();
app.UseSwagger();
app.UseSwaggerUI();

//app.UseHttpsRedirection();
app.MapControllers();

app.Run();

public partial class Program
{
    
}
