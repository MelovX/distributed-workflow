using DistributedWorkflow.Numbering.Grpc;
using DistributedWorkflow.Worker;
using DistributedWorkflow.Worker.Configuration;
using DistributedWorkflow.Worker.Metrics;
using DistributedWorkflow.Worker.Services;
using Microsoft.AspNetCore.Builder;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

var numberingGrpcAddressValue =
    builder.Configuration["NumberingGrpc:Address"]
    ?? throw new InvalidOperationException(
        "NumberingGrpc:Address is not configured.");

if (!Uri.TryCreate(
        numberingGrpcAddressValue,
        UriKind.Absolute,
        out var numberingGrpcAddress))
{
    throw new InvalidOperationException(
        "NumberingGrpc:Address must be a valid absolute URI.");
}

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics
            .AddMeter(WorkerMetrics.MeterName)
            .AddView(
                WorkerMetrics.RegistrationProcessingDurationName,
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
                WorkerMetrics.NumberingRequestDurationName,
                CreateLatencyHistogramConfiguration())
            .AddView(
                WorkerMetrics.KafkaPublishDurationName,
                CreateLatencyHistogramConfiguration())
            .AddView(
                WorkerMetrics.RabbitMqAcknowledgementDurationName,
                CreateLatencyHistogramConfiguration())
            .AddHttpClientInstrumentation()
            .AddRuntimeInstrumentation()
            .AddPrometheusExporter();
    });

builder.Services
    .AddOptions<RabbitMqOptions>()
    .Bind(builder.Configuration.GetSection(RabbitMqOptions.SectionName))
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
    .Validate(
        options => options.ConsumerConcurrency > 0,
        "RabbitMq:ConsumerConcurrency must be greater than zero.")
    .ValidateOnStart();

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

builder.Services.AddHostedService<Worker>();
builder.Services.AddSingleton<KafkaDocumentEventPublisher>();

builder.Services.AddGrpcClient<NumberingService.NumberingServiceClient>(
    options =>
    {
        options.Address = numberingGrpcAddress;
    });

var app = builder.Build();

app.UseOpenTelemetryPrometheusScrapingEndpoint();

app.Run();

static ExplicitBucketHistogramConfiguration CreateLatencyHistogramConfiguration()
{
    return new ExplicitBucketHistogramConfiguration
    {
        Boundaries =
        [
            0.0001,
            0.00025,
            0.0005,
            0.001,
            0.002,
            0.005,
            0.010,
            0.025,
            0.050,
            0.100,
            0.250,
            0.500,
            1.000
        ]
    };
}
