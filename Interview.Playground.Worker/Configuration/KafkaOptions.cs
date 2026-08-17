namespace Interview.Playground.Worker.Configuration
{
    public sealed class KafkaOptions
    {
        public const string SectionName = "Kafka";

        public string BootstrapServers { get; init; } = string.Empty;

        public string TopicName { get; init; } = string.Empty;
    }
}
