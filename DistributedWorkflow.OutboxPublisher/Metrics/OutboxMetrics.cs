using System.Diagnostics.Metrics;

namespace DistributedWorkflow.OutboxPublisher.Metrics;

public static class OutboxMetrics
{
    public const string MeterName = "DistributedWorkflow.OutboxPublisher";

    private static readonly Meter Meter = new(MeterName, "1.0.0");

    public static readonly Counter<long> Published =
        Meter.CreateCounter<long>("outbox.published");

    public static readonly Counter<long> Failed =
        Meter.CreateCounter<long>("outbox.failed");

    public static readonly Counter<long> OwnershipConflicts =
        Meter.CreateCounter<long>("outbox.ownership.conflicts");

    public static readonly Histogram<double> PublishBatchDuration =
        Meter.CreateHistogram<double>(
            "outbox.publish.batch.duration",
            unit: "s");

    public static readonly Histogram<int> PublishBatchSize =
        Meter.CreateHistogram<int>(
            "outbox.publish.batch.size",
            unit: "{message}");
}
