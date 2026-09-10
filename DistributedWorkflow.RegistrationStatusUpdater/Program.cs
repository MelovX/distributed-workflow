using Confluent.Kafka;
using DistributedWorkflow.RegistrationStatusUpdater.Configuration;
using DistributedWorkflow.RegistrationStatusUpdater.Data;
using DistributedWorkflow.RegistrationStatusUpdater.HealthChecks;
using DistributedWorkflow.RegistrationStatusUpdater.Metrics;
using DistributedWorkflow.RegistrationStatusUpdater.Services;
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
            .AddMeter(StatusUpdaterMetrics.MeterName)
            .AddMeter("Npgsql")
            .AddAspNetCoreInstrumentation()
            .AddRuntimeInstrumentation()
            .AddView(
                StatusUpdaterMetrics.BatchDurationName,
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
                StatusUpdaterMetrics.BatchSizeName,
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
                        1_000
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

var registrationDbConnectionString =
    builder.Configuration.GetConnectionString("RegistrationDb")
    ?? throw new InvalidOperationException(
        "Connection string 'RegistrationDb' is not configured.");

builder.Services.AddSingleton(
    _ => NpgsqlDataSource.Create(registrationDbConnectionString));

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.BootstrapServers),
        "Kafka:BootstrapServers is required.")
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.TopicName),
        "Kafka:TopicName is required.")
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.GroupId),
        "Kafka:GroupId is required.")
    .Validate(
        options => options.ConsumerCount is > 0 and <= 32,
        "Kafka:ConsumerCount must be between 1 and 32.")
    .Validate(
        options => options.BatchSize is > 0 and <= 10_000,
        "Kafka:BatchSize must be between 1 and 10000.")
    .Validate(
        options => options.BatchDelay > TimeSpan.Zero,
        "Kafka:BatchDelay must be greater than zero.")
    .Validate(
        options => options.ErrorDelay > TimeSpan.Zero,
        "Kafka:ErrorDelay must be greater than zero.")
    .ValidateOnStart();

builder.Services.AddSingleton(
    serviceProvider =>
    {
        var kafkaOptions = serviceProvider
            .GetRequiredService<IOptions<KafkaOptions>>()
            .Value;

        return new AdminClientBuilder(
            new AdminClientConfig
            {
                BootstrapServers = kafkaOptions.BootstrapServers,
                ClientId =
                    $"{Environment.MachineName}-status-updater-health"
            })
            .Build();
    });

builder.Services.AddSingleton<RegistrationStatusStore>();

var consumerCount = builder.Configuration.GetValue(
    "Kafka:ConsumerCount",
    1);

if (consumerCount < 1)
{
    throw new InvalidOperationException(
        "Kafka:ConsumerCount must be greater than zero.");
}

for (var index = 0; index < consumerCount; index++)
{
    var consumerIndex = index;

    builder.Services.AddSingleton<IHostedService>(
        serviceProvider =>
            ActivatorUtilities.CreateInstance<RegistrationStatusConsumer>(
                serviceProvider,
                consumerIndex));
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
