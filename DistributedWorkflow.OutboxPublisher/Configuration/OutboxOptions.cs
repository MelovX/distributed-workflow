namespace DistributedWorkflow.OutboxPublisher.Configuration
{
    public sealed class OutboxOptions
    {
        public const string SectionName = "Outbox";

        public int DispatcherCount { get; init; } = 1;

        public int BatchSize { get; init; } = 100;

        public TimeSpan LeaseDuration { get; init; } =
        TimeSpan.FromSeconds(20);

        public TimeSpan EmptyBatchDelay { get; init; } =
            TimeSpan.FromSeconds(5);
    }
}
