namespace DistributedWorkflow.OutboxPublisher.Entities
{
    public sealed record OutboxPublishResult(
        Guid MessageId,
        bool IsConfirmed,
        Exception? Failure);
}
