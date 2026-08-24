using Grpc.Core;

namespace DistributedWorkflow.Numbering.Grpc.Services;

public sealed class NumberingGrpcService : NumberingService.NumberingServiceBase
{
    private static int _counter = 0;

    private readonly ILogger<NumberingGrpcService> _logger;

    public NumberingGrpcService(ILogger<NumberingGrpcService> logger)
    {
        _logger = logger;
    }

    public override Task<ReserveNumberResponse> ReserveNumber(
        ReserveNumberRequest request,
        ServerCallContext context)
    {
        var next = Interlocked.Increment(ref _counter);

        var number = $"REG-2026-{next:000000}";

        _logger.LogInformation(
            "Reserved number {Number} for DocumentId={DocumentId}, OperationId={OperationId}",
            number,
            request.DocumentId,
            request.OperationId);

        return Task.FromResult(new ReserveNumberResponse
        {
            Number = number,
            Status = "Reserved"
        });
    }
}