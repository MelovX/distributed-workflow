namespace DistributedWorkflow.Worker.Configuration
{
    public sealed class RabbitMqOptions
    {
        public const string SectionName = "RabbitMq";

        public string HostName { get; init; } = string.Empty;

        public int Port { get; init; }

        public string UserName { get; init; } = string.Empty;

        public string Password { get; init; } = string.Empty;

        public ushort ConsumerConcurrency { get; init; } = 8;
    }
}
