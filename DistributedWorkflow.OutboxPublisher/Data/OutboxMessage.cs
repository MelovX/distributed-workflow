namespace DistributedWorkflow.OutboxPublisher.Data
{
    public sealed class OutboxMessage
    {
        public Guid Id { get; set; }

        public string Type { get; set; } = default!;

        public string Payload { get; set; } = default!;

        public DateTimeOffset CreatedAt { get; set; }

        public DateTimeOffset? PublishedAt { get; set; }

        public int Attempts { get; set; }

        public string? LastError { get; set; }

        public DateTimeOffset? LockedUntil { get; set; }

        public string? LockedBy { get; set; }
    }
}
