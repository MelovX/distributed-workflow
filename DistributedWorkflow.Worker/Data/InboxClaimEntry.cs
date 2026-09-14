namespace DistributedWorkflow.Worker.Data
{
    public sealed record InboxClaimEntry(
        Guid MessageId,
        string OperationId,
        string MessageType,
        string LockOwner);
}
