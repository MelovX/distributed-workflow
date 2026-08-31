namespace DistributedWorkflow.Api.Batching
{
    public sealed class RegistrationBatchOptions
    {
        public const string SectionName = "RegistrationBatch";
        public int MaxBatchSize { get; set; } = 64;
        public TimeSpan MaxBatchDelay { get; set; } =
            TimeSpan.FromMilliseconds(5);
        public int Capacity { get; set; } = 20000;
    }
}
