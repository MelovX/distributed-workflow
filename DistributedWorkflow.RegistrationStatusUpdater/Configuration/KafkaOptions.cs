namespace DistributedWorkflow.RegistrationStatusUpdater.Configuration
{
    public sealed class KafkaOptions
    {
        public const string SectionName = "Kafka";

        public string BootstrapServers { get; init; } = string.Empty;

        public string TopicName { get; init; } = string.Empty;

        public string GroupId { get; init; } = string.Empty;

        public int ConsumerCount { get; init; } = 2;

        public int BatchSize { get; init; } = 500;

        public TimeSpan BatchDelay { get; init; } =
            TimeSpan.FromMilliseconds(10);

        public TimeSpan ErrorDelay { get; init; } =
            TimeSpan.FromSeconds(5);
    }
}
