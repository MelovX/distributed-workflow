namespace DistributedWorkflow.Api.Batching
{
    public sealed record OutboxMessageWriteModel(
        Guid Id,
        string Type,
        string Payload,
        DateTimeOffset CreatedAt);
}
