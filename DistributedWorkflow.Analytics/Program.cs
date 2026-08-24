using DistributedWorkflow.Analytics;
using DistributedWorkflow.Analytics.Configuration;

var builder = Host.CreateApplicationBuilder(args);

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
    .ValidateOnStart();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
