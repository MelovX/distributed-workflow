using System.Diagnostics.Metrics;

namespace DistributedWorkflow.RegistrationStatusUpdater.Metrics
{
    public static class StatusUpdaterMetrics
    {
        public const string MeterName =
            "DistributedWorkflow.RegistrationStatusUpdater";

        public const string BatchDurationName =
            "status.updater.batch.duration";

        public const string BatchSizeName =
            "status.updater.batch.size";

        private static readonly Meter Meter =
            new(MeterName, "1.0.0");

        public static readonly Counter<long> EventsConsumed =
            Meter.CreateCounter<long>(
                "status.updater.events.consumed",
                unit: "{event}");

        public static readonly Counter<long> OperationsUpdated =
            Meter.CreateCounter<long>(
                "status.updater.operations.updated",
                unit: "{operation}");

        public static readonly Counter<long> OperationsUnchanged =
            Meter.CreateCounter<long>(
                "status.updater.operations.unchanged",
                unit: "{operation}");

        public static readonly Counter<long> InvalidEvents =
            Meter.CreateCounter<long>(
                "status.updater.events.invalid",
                unit: "{event}");

        public static readonly Counter<long> IterationErrors =
            Meter.CreateCounter<long>(
                "status.updater.iteration.errors",
                unit: "{error}");

        public static readonly Histogram<int> BatchSize =
            Meter.CreateHistogram<int>(
                BatchSizeName,
                unit: "{event}");

        public static readonly Histogram<double> BatchDuration =
            Meter.CreateHistogram<double>(
                BatchDurationName,
                unit: "s");
    }
}
