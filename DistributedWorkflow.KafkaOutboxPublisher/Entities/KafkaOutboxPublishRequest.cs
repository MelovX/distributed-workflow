namespace DistributedWorkflow.KafkaOutboxPublisher.Entities
{
    public sealed record KafkaOutboxPublishRequest(
        Guid MessageId,
        string OperationId,
        string MessageType,
        string PartitionKey,
        string Payload);
}
