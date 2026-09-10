namespace DistributedWorkflow.Worker.Data.Entities
{
    public sealed class InboxMessage
    {
        public Guid MessageId { get; set; }

        public string OperationId { get; set; } = default!;

        public string MessageType { get; set; } = default!;

        public DateTimeOffset ReceivedAt { get; set; }

        public DateTimeOffset? CompletedAt { get; set; }

        public int Attempts { get; set; }

        public DateTimeOffset? LockedUntil { get; set; }

        public string? LockedBy { get; set; }

        public string? LastError { get; set; }
    }
}
