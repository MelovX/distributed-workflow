using Interview.Playground.Api.Configuration;
using Interview.Playground.Api.Data;
using Interview.Playground.Api.Metrics;
using Interview.Playground.Api.Services;
using Microsoft.EntityFrameworkCore;
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
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddRuntimeInstrumentation()
            .AddPrometheusExporter();
    });

var redisConnectionString =
    builder.Configuration.GetConnectionString("Redis")
    ?? throw new InvalidOperationException(
        "Connection string 'Redis' is not configured.");
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

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddControllers();
builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect(redisConnectionString));
builder.Services.AddSingleton<RabbitMqRegistrationPublisher>();
builder.Services.AddHostedService<OutboxDispatcher>();
builder.Services.AddDbContext<RegistrationDbContext>(options =>
{
    options.UseNpgsql(registrationDbConnectionString);
});
builder.Services.AddHealthChecks();

var app = builder.Build();

app.MapHealthChecks("/health/live");
app.UseOpenTelemetryPrometheusScrapingEndpoint();
app.UseSwagger();
app.UseSwaggerUI();

//app.UseHttpsRedirection();
app.MapControllers();

app.Run();
