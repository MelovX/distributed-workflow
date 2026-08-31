namespace DistributedWorkflow.Api.Batching
{
    internal interface IRegistrationBatchStore
    {
        Task<IReadOnlyList<RegistrationWriteResult>> WriteAsync(
            IReadOnlyList<RegistrationWriteRequest> requests,
            CancellationToken cancellationToken);
    }
}
