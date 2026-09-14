namespace DistributedWorkflow.Worker.Data
{
    public sealed record InboxClaimResult(
        Guid MessageId,
        string LockOwner,
        InboxClaimStatus Status);
}
