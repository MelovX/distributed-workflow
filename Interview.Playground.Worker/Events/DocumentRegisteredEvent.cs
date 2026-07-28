namespace Interview.Playground.Worker.Events
{
    public sealed class DocumentRegisteredEvent
    {
        public required string EventId { get; init; }

        public required string OperationId { get; init; }

        public required string DocumentId { get; init; }

        public required string Title { get; init; }

        public required DateTimeOffset RegisteredAt { get; init; }
    }
}
