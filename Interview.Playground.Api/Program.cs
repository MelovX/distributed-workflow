using Interview.Playground.Api.Data;
using Interview.Playground.Api.Services;
using Microsoft.EntityFrameworkCore;
using StackExchange.Redis;
using OpenTelemetry.Metrics;
using Interview.Playground.Api.Metrics;

var builder = WebApplication.CreateBuilder(args);

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
// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddControllers();
builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect("localhost:6379"));
builder.Services.AddSingleton<RabbitMqRegistrationPublisher>();
builder.Services.AddHostedService<OutboxDispatcher>();
builder.Services.AddDbContext<RegistrationDbContext>(options =>
{
    options.UseNpgsql(builder.Configuration.GetConnectionString("RegistrationDb"));
});

var app = builder.Build();

app.UseOpenTelemetryPrometheusScrapingEndpoint();
app.UseSwagger();
app.UseSwaggerUI();

//app.UseHttpsRedirection();
app.MapControllers();

app.Run();
