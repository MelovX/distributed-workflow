using Confluent.Kafka;
using DistributedWorkflow.KafkaOutboxPublisher.Configuration;
using DistributedWorkflow.KafkaOutboxPublisher.Data;
using DistributedWorkflow.KafkaOutboxPublisher.HealthChecks;
using DistributedWorkflow.KafkaOutboxPublisher.Metrics;
using DistributedWorkflow.KafkaOutboxPublisher.Services;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Npgsql;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics
            .AddMeter(KafkaOutboxMetrics.MeterName)
            .AddMeter("Npgsql")
            .AddAspNetCoreInstrumentation()
            .AddRuntimeInstrumentation()
            .AddView(
                KafkaOutboxMetrics.BatchDurationName,
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
                        5.000,
                        10.000
                    ]
                })
            .AddView(
                KafkaOutboxMetrics.BatchSizeName,
                new ExplicitBucketHistogramConfiguration
                {
                    Boundaries =
                    [
                        1,
                        10,
                        50,
                        100,
                        250,
                        500,
                        1_000,
                        2_500,
                        5_000,
                        10_000
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

var workerDbConnectionString =
    builder.Configuration.GetConnectionString("WorkerDb")
    ?? throw new InvalidOperationException(
        "Connection string 'WorkerDb' is not configured.");

builder.Services.AddSingleton(
    _ => NpgsqlDataSource.Create(workerDbConnectionString));

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.BootstrapServers),
        "Kafka:BootstrapServers is required.")
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.TopicName),
        "Kafka:TopicName is required.")
    .ValidateOnStart();

builder.Services.AddSingleton(
    serviceProvider =>
    {
        var kafkaOptions =
            serviceProvider
                .GetRequiredService<IOptions<KafkaOptions>>()
                .Value;

        return new AdminClientBuilder(
            new AdminClientConfig
            {
                BootstrapServers =
                    kafkaOptions.BootstrapServers,
                ClientId =
                    $"{Environment.MachineName}-kafka-outbox-health"
            })
            .Build();
    });

builder.Services
    .AddOptions<OutboxOptions>()
    .Bind(builder.Configuration.GetSection(OutboxOptions.SectionName))
    .Validate(
        options => options.DispatcherCount is > 0 and <= 32,
        "Outbox:DispatcherCount must be between 1 and 32.")
    .Validate(
        options => options.BatchSize is > 0 and <= 10_000,
        "Outbox:BatchSize must be between 1 and 10000.")
    .Validate(
        options => options.LeaseDuration > TimeSpan.Zero,
        "Outbox:LeaseDuration must be greater than zero.")
    .Validate(
        options => options.EmptyBatchDelay > TimeSpan.Zero,
        "Outbox:EmptyBatchDelay must be greater than zero.")
    .Validate(
        options => options.ErrorDelay > TimeSpan.Zero,
        "Outbox:ErrorDelay must be greater than zero.")
    .ValidateOnStart();

builder.Services.AddSingleton<KafkaOutboxStore>();
builder.Services.AddSingleton<KafkaBatchPublisher>();

var dispatcherCount = builder.Configuration.GetValue(
    "Outbox:DispatcherCount",
    1);

if (dispatcherCount < 1)
{
    throw new InvalidOperationException(
        "Outbox:DispatcherCount must be greater than zero.");
}

for (var index = 0; index < dispatcherCount; index++)
{
    builder.Services.AddSingleton<IHostedService>(
        serviceProvider =>
            ActivatorUtilities.CreateInstance<OutboxDispatcher>(
                serviceProvider));
}

builder.Services.AddHealthChecks()
    .AddCheck<PostgresHealthCheck>(
        "postgres",
        failureStatus: HealthStatus.Unhealthy,
        tags: ["ready"])
    .AddCheck<KafkaHealthCheck>(
        "kafka",
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
