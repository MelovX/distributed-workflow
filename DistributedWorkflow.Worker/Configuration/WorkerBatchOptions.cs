namespace DistributedWorkflow.Worker.Configuration
{
    public sealed class WorkerBatchOptions
    {
        public const string SectionName = "WorkerBatch";

        public int MaxBatchSize { get; init; } = 100;

        public TimeSpan MaxBatchDelay { get; init; } =
            TimeSpan.FromMilliseconds(5);

        public int Capacity { get; init; } = 2048;
    }
}
