using DistributedWorkflow.OutboxPublisher.Configuration;
using DistributedWorkflow.OutboxPublisher.Data;
using DistributedWorkflow.OutboxPublisher.HealthChecks;
using DistributedWorkflow.OutboxPublisher.Metrics;
using DistributedWorkflow.OutboxPublisher.Services;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics
            .AddMeter(OutboxMetrics.MeterName)
            .AddMeter("Npgsql")
            .AddAspNetCoreInstrumentation()
            .AddRuntimeInstrumentation()
            .AddView(
                "outbox.publish.batch.duration",
                new ExplicitBucketHistogramConfiguration
                {
                    Boundaries =
                    [
                        0.001,
                        0.002,
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
            .AddView(
                "outbox.publish.batch.size",
                new ExplicitBucketHistogramConfiguration
                {
                    Boundaries =
                    [
                        1,
                        10,
                        25,
                        50,
                        75,
                        100,
                        128
                    ]
                })
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
            .AddPrometheusExporter();
    });

var outboxDbConnectionString =
    builder.Configuration.GetConnectionString("OutboxDb")
    ?? throw new InvalidOperationException(
        "Connection string 'OutboxDb' is not configured.");

builder.Services.AddDbContext<OutboxDbContext>(options =>
{
    options.UseNpgsql(outboxDbConnectionString);
});
builder.Services
    .AddOptions<RabbitMqOptions>()
    .Bind(builder.Configuration.GetSection(
        RabbitMqOptions.SectionName))
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.HostName),
        "RabbitMq:HostName is required.")
    .Validate(
        options => options.Port is > 0 and <= 65535,
        "RabbitMq:Port must be a valid TCP port.")
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.UserName),
        "RabbitMq:UserName is required.")
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.Password),
        "RabbitMq:Password is required.")
    .ValidateOnStart();

var dispatcherCount = builder.Configuration.GetValue(
    "Outbox:DispatcherCount",
    1);

if (dispatcherCount < 1)
{
    throw new InvalidOperationException(
        "Outbox:DispatcherCount must be greater than zero.");
}

builder.Services.AddSingleton<RabbitMqConnectionProvider>();
builder.Services.AddTransient<RabbitMqOutboxPublisher>();

for (var index = 0; index < dispatcherCount; index++)
{
    builder.Services.AddSingleton<IHostedService>(serviceProvider =>
        ActivatorUtilities.CreateInstance<OutboxDispatcher>(serviceProvider));
}

builder.Services.AddHealthChecks()
    .AddCheck<PostgresHealthCheck>(
        "postgres",
        failureStatus: HealthStatus.Unhealthy,
        tags: ["ready"]);

var app = builder.Build();

app.UseOpenTelemetryPrometheusScrapingEndpoint();

app.MapHealthChecks(
    "/health/live",
    new HealthCheckOptions
    {
        Predicate = _ => false
    });
app.MapHealthChecks(
    "/health/ready",
    new HealthCheckOptions
    {
        Predicate = healthCheck =>
            healthCheck.Tags.Contains("ready")
    });

app.Run();
