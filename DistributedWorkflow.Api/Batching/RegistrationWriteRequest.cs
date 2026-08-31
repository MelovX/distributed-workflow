namespace DistributedWorkflow.Api.Batching
{
    public sealed record RegistrationWriteRequest(
        RegistrationOperationWriteModel Operation,
        OutboxMessageWriteModel OutboxMessage);
}
