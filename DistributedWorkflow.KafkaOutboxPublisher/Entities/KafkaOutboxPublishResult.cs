namespace DistributedWorkflow.KafkaOutboxPublisher.Entities
{
    public sealed record KafkaOutboxPublishResult(
        Guid MessageId,
        bool IsPublished,
        Exception? Failure);
}
