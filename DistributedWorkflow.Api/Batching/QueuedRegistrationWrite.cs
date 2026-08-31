using DistributedWorkflow.Api.Models;

namespace DistributedWorkflow.Api.Batching
{
    internal sealed class QueuedRegistrationWrite
    {
        private readonly TaskCompletionSource<RegisterDocumentResponse>
            _completionSource;

        public QueuedRegistrationWrite(
            RegistrationWriteRequest request)
        {
            Request = request;

            _completionSource =
                new TaskCompletionSource<RegisterDocumentResponse>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public RegistrationWriteRequest Request { get; }
        public Task<RegisterDocumentResponse> Completion =>
            _completionSource.Task;

        public void Complete(RegisterDocumentResponse response)
        {
            _completionSource.TrySetResult(response);
        }

        public void Fail(Exception exception)
        {
            _completionSource.TrySetException(exception);
        }
    }
}
