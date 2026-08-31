using DistributedWorkflow.Api.Batching;
using DistributedWorkflow.Api.Models;

namespace DistributedWorkflow.Api.UnitTests.Batching
{
    public sealed class RegistrationWriteQueueTests
    {
        [Fact]
        public async Task EnqueueAsync_WhenQueuedWriteCompletes_ReturnsItsResponse()
        {
            var options = RegistrationBatchTestData.CreateOptions(
                maxBatchSize: 10,
                maxBatchDelay: TimeSpan.FromMilliseconds(5));

            var queue = new RegistrationWriteQueue(options);
            var request = RegistrationBatchTestData.CreateRequest();
            var cancellationToken = TestContext.Current.CancellationToken;

            var enqueueTask = queue.EnqueueAsync(
                request,
                cancellationToken);

            var queuedWrite = await queue.Reader.ReadAsync(
                cancellationToken);

            Assert.False(enqueueTask.IsCompleted);
            Assert.Same(request, queuedWrite.Request);

            var expectedResponse = new RegisterDocumentResponse(
                request.Operation.Id,
                request.Operation.Status);

            queuedWrite.Complete(expectedResponse);

            var response = await enqueueTask.WaitAsync(
                TimeSpan.FromSeconds(2),
                cancellationToken);

            Assert.Equal(expectedResponse, response);
        }

        [Fact]
        public async Task EnqueueAsync_WhenRequestIsCancelled_StopsWaitingForResponse()
        {
            var options = RegistrationBatchTestData.CreateOptions(
                maxBatchSize: 10,
                maxBatchDelay: TimeSpan.FromMilliseconds(5));

            var queue = new RegistrationWriteQueue(options);
            var request = RegistrationBatchTestData.CreateRequest();

            using var cancellationTokenSource =
                new CancellationTokenSource();

            var enqueueTask = queue.EnqueueAsync(
                request,
                cancellationTokenSource.Token);

            await queue.Reader.ReadAsync(
                TestContext.Current.CancellationToken);

            cancellationTokenSource.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => enqueueTask);
        }
    }
}
