namespace DistributedWorkflow.KafkaOutboxPublisher.Entities
{
    public sealed record ClaimedOutboxBatch(
        string LockOwner,
        IReadOnlyList<KafkaOutboxPublishRequest> Messages);
}
