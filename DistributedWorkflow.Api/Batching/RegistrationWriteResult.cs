namespace DistributedWorkflow.Api.Batching
{
    internal sealed record RegistrationWriteResult(
        string OperationId,
        string Status);
}
