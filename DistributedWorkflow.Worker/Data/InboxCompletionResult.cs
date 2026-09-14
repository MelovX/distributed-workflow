namespace DistributedWorkflow.Worker.Data
{
    public sealed record InboxCompletionResult(
        Guid MessageId,
        string LockOwner,
        bool Completed);
}
