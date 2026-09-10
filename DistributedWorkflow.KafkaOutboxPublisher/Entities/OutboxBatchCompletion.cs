namespace DistributedWorkflow.KafkaOutboxPublisher.Entities
{
    public sealed record OutboxBatchCompletion(
        int PublishedCount,
        int FailedCount,
        int OwnershipConflictCount);
}
