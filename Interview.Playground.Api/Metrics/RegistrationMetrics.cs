using System.Diagnostics.Metrics;

namespace Interview.Playground.Api.Metrics
{
    public class RegistrationMetrics
    {
        public const string MeterName = "Interview.Playground.Registration";

        public static readonly Meter Meter = new(MeterName, "1.0.0");

        public static readonly Counter<long> RegistrationsCreated =
            Meter.CreateCounter<long>("registrations.created");

        public static readonly Counter<long> OutboxPublished =
            Meter.CreateCounter<long>("outbox.published");

        public static readonly Counter<long> OutboxFailed =
            Meter.CreateCounter<long>("outbox.failed");
    }
}
