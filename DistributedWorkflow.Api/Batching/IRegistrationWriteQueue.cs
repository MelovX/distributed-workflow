using DistributedWorkflow.Api.Models;

namespace DistributedWorkflow.Api.Batching
{
    public interface IRegistrationWriteQueue
    {
        Task<RegisterDocumentResponse> EnqueueAsync(
            RegistrationWriteRequest request,
            CancellationToken cancellationToken);
    }
}
