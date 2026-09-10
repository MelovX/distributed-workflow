namespace DistributedWorkflow.Worker.Data
{
    public sealed record KafkaOutboxEntry(
        Guid Id,
        string OperationId,
        string MessageType,
        string PartitionKey,
        string Payload);
}
