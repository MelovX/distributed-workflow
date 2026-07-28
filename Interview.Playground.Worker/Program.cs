using Interview.Playground.Numbering.Grpc;
using Interview.Playground.Worker;
using Interview.Playground.Worker.Data;
using Interview.Playground.Worker.Services;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddDbContext<RegistrationDbContext>(options =>
{
    options.UseNpgsql(builder.Configuration.GetConnectionString("RegistrationDb"));
});
builder.Services.AddHostedService<Worker>();
builder.Services.AddSingleton<KafkaDocumentEventPublisher>();
builder.Services.AddGrpcClient<NumberingService.NumberingServiceClient>(options =>
{
    options.Address = new Uri("https://localhost:7158");
});

var host = builder.Build();
host.Run();
