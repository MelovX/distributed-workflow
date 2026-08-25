using System.Diagnostics.Metrics;

namespace DistributedWorkflow.Worker.Metrics
{
    public static class WorkerMetrics
    {
        public const string MeterName = "DistributedWorkflow.Worker";
        public const string RegistrationProcessingDurationName =
            "worker.registration.processing.duration";

        public static readonly Meter Meter = new(MeterName, "1.0.0");

        public static readonly Counter<long> RegistrationsCompleted =
            Meter.CreateCounter<long>("worker.registrations.completed", unit: "{registration}");

        public static readonly Histogram<double> RegistrationProcessingDuration =
            Meter.CreateHistogram<double>(RegistrationProcessingDurationName, unit: "s");
    }
}
