using DistributedWorkflow.Api.Batching;
using DistributedWorkflow.Api.Data;
using DistributedWorkflow.Api.HealthChecks;
using DistributedWorkflow.Api.Metrics;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;
using OpenTelemetry.Metrics;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var registrationDbConnectionString =
    builder.Configuration.GetConnectionString("RegistrationDb")
    ?? throw new InvalidOperationException(
        "Connection string 'RegistrationDb' is not configured.");

builder.Services.AddSingleton(
    _ => NpgsqlDataSource.Create(registrationDbConnectionString));
builder.Services.AddSingleton<RegistrationQueryStore>();

builder.Services
    .AddOptions<RegistrationBatchOptions>()
    .Bind(
        builder.Configuration.GetSection(
            RegistrationBatchOptions.SectionName))
    .Validate(
        options => options.MaxBatchSize > 0,
        "RegistrationBatch:MaxBatchSize must be greater than zero.")
    .Validate(
        options => options.MaxBatchDelay > TimeSpan.Zero,
        "RegistrationBatch:MaxBatchDelay must be greater than zero.")
    .Validate(
        options => options.Capacity >= options.MaxBatchSize,
        "RegistrationBatch:Capacity must be greater than or equal to MaxBatchSize.")
    .ValidateOnStart();
builder.Services.AddSingleton<RegistrationWriteQueue>();
builder.Services.AddSingleton<IRegistrationWriteQueue>(
    serviceProvider =>
        serviceProvider.GetRequiredService<RegistrationWriteQueue>());
builder.Services.AddSingleton<RegistrationBatchReader>();
builder.Services.AddSingleton<
    IRegistrationBatchStore, RegistrationBatchStore>();
builder.Services.AddHostedService<
    RegistrationBatchWriter>();

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

app.MapControllers();

app.Run();

public partial class Program
{

}
