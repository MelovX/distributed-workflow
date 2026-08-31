namespace DistributedWorkflow.Api.Batching
{
    public sealed record RegistrationOperationWriteModel(
        string Id,
        string IdempotencyKey,
        string DocumentId,
        string Title,
        string Status,
        DateTimeOffset CreatedAt);
}
