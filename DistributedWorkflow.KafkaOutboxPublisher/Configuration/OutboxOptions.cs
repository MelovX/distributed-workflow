namespace DistributedWorkflow.KafkaOutboxPublisher.Configuration
{
    public sealed class OutboxOptions
    {
        public const string SectionName = "Outbox";

        public int DispatcherCount { get; init; } = 2;

        public int BatchSize { get; init; } = 500;

        public TimeSpan LeaseDuration { get; init; } =
            TimeSpan.FromSeconds(30);

        public TimeSpan EmptyBatchDelay { get; init; } =
            TimeSpan.FromSeconds(1);

        public TimeSpan ErrorDelay { get; init; } =
            TimeSpan.FromSeconds(5);
    }
}
