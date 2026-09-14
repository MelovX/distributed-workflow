namespace DistributedWorkflow.Worker.Data
{
    public sealed record InboxCompletionEntry(
        Guid MessageId,
        string LockOwner,
        KafkaOutboxEntry OutboxEntry);
}
