namespace DistributedWorkflow.OutboxPublisher.Entities
{
    public sealed record OutboxPublishRequest(
        Guid MessageId,
        string MessageType,
        string Payload);
}
