using NBomber.CSharp;
using NBomber.Http.CSharp;

var baseUrl = Environment.GetEnvironmentVariable("LOAD_TEST_BASE_URL")
    ?? "http://localhost:5268";
var rate = ReadPositiveInteger("LOAD_TEST_RATE", defaultValue: 2000);
var durationSeconds = ReadPositiveInteger(
    "LOAD_TEST_DURATION_SECONDS",
    defaultValue: 300);

var httpClient = Http.CreateDefaultClient(maxConnectionsPerServer: 1024);

var scenario = Scenario.Create("register_document", async _ =>
{
    var requestId = Guid.NewGuid().ToString("N");

    var request = Http.CreateRequest(
            "POST",
            $"{baseUrl}/registrations")
        .WithHeader("Accept", "application/json")
        .WithHeader("Idempotency-Key", requestId)
        .WithJsonBody(new RegisterDocumentRequest(
            DocumentId: $"document-{requestId}",
            Title: "Load test document"));

    return await Http.Send(httpClient, request);
})
.WithLoadSimulations(
    Simulation.RampingInject(
        rate: rate,
        interval: TimeSpan.FromSeconds(1),
        during: TimeSpan.FromSeconds(30)),
    Simulation.Inject(
        rate: rate,
        interval: TimeSpan.FromSeconds(1),
        during: TimeSpan.FromSeconds(durationSeconds)));

Console.WriteLine(
    $"Target: {baseUrl}; rate: {rate} req/s; duration: {durationSeconds}s");

NBomberRunner
    .RegisterScenarios(scenario)
    .Run();

static int ReadPositiveInteger(string variableName, int defaultValue)
{
    var rawValue = Environment.GetEnvironmentVariable(variableName);

    return int.TryParse(rawValue, out var value) && value > 0
        ? value
        : defaultValue;
}

internal sealed record RegisterDocumentRequest(
    string DocumentId,
    string Title);
