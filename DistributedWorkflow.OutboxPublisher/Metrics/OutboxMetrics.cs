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

    public static readonly Histogram<double> PublishDuration =
        Meter.CreateHistogram<double>(
            "outbox.publish.duration",
            unit: "s");
}
