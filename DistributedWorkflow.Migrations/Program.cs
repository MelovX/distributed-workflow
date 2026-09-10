using DistributedWorkflow.Api.Data;
using DistributedWorkflow.Worker.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

var registrationDbConnectionString =
    builder.Configuration.GetConnectionString("RegistrationDb")
    ?? throw new InvalidOperationException(
        "Connection string 'RegistrationDb' is not configured.");

var workerDbConnectionString =
    builder.Configuration.GetConnectionString("WorkerDb")
    ?? throw new InvalidOperationException(
        "Connection string 'WorkerDb' is not configured.");

builder.Services.AddDbContext<RegistrationDbContext>(
    options =>
        options.UseNpgsql(registrationDbConnectionString));

builder.Services.AddDbContext<WorkerDbContext>(
    options =>
        options.UseNpgsql(workerDbConnectionString));

using var host = builder.Build();

await using var scope =
    host.Services.CreateAsyncScope();

var loggerFactory = scope.ServiceProvider
    .GetRequiredService<ILoggerFactory>();

var logger = loggerFactory.CreateLogger(
    "DatabaseMigrations");

logger.LogInformation(
    "Applying Registration database migrations.");

var registrationDbContext = scope.ServiceProvider
    .GetRequiredService<RegistrationDbContext>();

await registrationDbContext.Database.MigrateAsync();

logger.LogInformation(
    "Registration database migrations applied.");

logger.LogInformation(
    "Applying Worker database migrations.");

var workerDbContext = scope.ServiceProvider
    .GetRequiredService<WorkerDbContext>();

await workerDbContext.Database.MigrateAsync();

logger.LogInformation(
    "Worker database migrations applied.");