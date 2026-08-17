using Interview.Playground.Numbering.Grpc;
using Interview.Playground.Worker;
using Interview.Playground.Worker.Configuration;
using Interview.Playground.Worker.Data;
using Interview.Playground.Worker.Services;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

var registrationDbConnectionString =
    builder.Configuration.GetConnectionString("RegistrationDb")
    ?? throw new InvalidOperationException(
        "Connection string 'RegistrationDb' is not configured.");

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

builder.Services.AddDbContext<RegistrationDbContext>(options =>
{
    options.UseNpgsql(registrationDbConnectionString);
});

builder.Services.AddHostedService<Worker>();
builder.Services.AddSingleton<KafkaDocumentEventPublisher>();

builder.Services.AddGrpcClient<NumberingService.NumberingServiceClient>(
    options =>
    {
        options.Address = numberingGrpcAddress;
    });

var host = builder.Build();
host.Run();
