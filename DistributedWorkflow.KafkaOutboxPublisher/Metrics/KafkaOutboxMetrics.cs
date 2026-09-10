using System.Diagnostics.Metrics;

namespace DistributedWorkflow.KafkaOutboxPublisher.Metrics
{
    public static class KafkaOutboxMetrics
    {
        public const string MeterName =
            "DistributedWorkflow.KafkaOutboxPublisher";

        public const string BatchDurationName =
            "kafka.outbox.batch.duration";

        public const string BatchSizeName =
            "kafka.outbox.batch.size";

        private static readonly Meter Meter =
            new(MeterName, "1.0.0");

        public static readonly Counter<long> Published =
            Meter.CreateCounter<long>(
                "kafka.outbox.published",
                unit: "{message}");

        public static readonly Counter<long> Failed =
            Meter.CreateCounter<long>(
                "kafka.outbox.failed",
                unit: "{message}");

        public static readonly Counter<long> OwnershipConflicts =
            Meter.CreateCounter<long>(
                "kafka.outbox.ownership.conflicts",
                unit: "{message}");

        public static readonly Counter<long> IterationErrors =
            Meter.CreateCounter<long>(
                "kafka.outbox.iteration.errors",
                unit: "{error}");

        public static readonly Histogram<int> BatchSize =
            Meter.CreateHistogram<int>(
                BatchSizeName,
                unit: "{message}");

        public static readonly Histogram<double> BatchDuration =
            Meter.CreateHistogram<double>(
                BatchDurationName,
                unit: "s");
    }
}
