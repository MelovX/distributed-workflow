namespace DistributedWorkflow.Worker.Data.Entities
{
    public sealed class KafkaOutboxMessage
    {
        public Guid Id { get; set; }

        public string OperationId { get; set; } = default!;

        public string MessageType { get; set; } = default!;

        public string PartitionKey { get; set; } = default!;

        public string Payload { get; set; } = default!;

        public DateTimeOffset CreatedAt { get; set; }

        public DateTimeOffset? PublishedAt { get; set; }

        public int Attempts { get; set; }

        public DateTimeOffset? LockedUntil { get; set; }

        public string? LockedBy { get; set; }

        public string? LastError { get; set; }
    }
}
