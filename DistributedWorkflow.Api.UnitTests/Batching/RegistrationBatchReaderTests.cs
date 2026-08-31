using DistributedWorkflow.Api.Batching;
using DistributedWorkflow.Api.Models;
using System.Diagnostics;

namespace DistributedWorkflow.Api.UnitTests.Batching
{
    public sealed class RegistrationBatchReaderTests
    {
        [Fact]
        public async Task ReadAsync_WhenMaximumSizeIsReached_ReturnsFullBatch()
        {
            var options = RegistrationBatchTestData.CreateOptions(
                maxBatchSize: 3,
                maxBatchDelay: TimeSpan.FromSeconds(5));

            var queue = new RegistrationWriteQueue(options);
            var reader = new RegistrationBatchReader(queue, options);
            var cancellationToken = TestContext.Current.CancellationToken;

            var requests = Enumerable.Range(0, 3)
                .Select(_ => RegistrationBatchTestData.CreateRequest())
                .ToArray();

            var enqueueTasks = requests
                .Select(request =>
                    queue.EnqueueAsync(request, cancellationToken))
                .ToArray();

            var batch = await reader.ReadAsync(cancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);

            Assert.Equal(3, batch.Count);

            CompleteBatch(batch);

            await Task.WhenAll(enqueueTasks);
        }

        [Fact]
        public async Task ReadAsync_WhenDelayExpires_ReturnsPartialBatch()
        {
            var maxBatchDelay = TimeSpan.FromMilliseconds(100);

            var options = RegistrationBatchTestData.CreateOptions(
                maxBatchSize: 10,
                maxBatchDelay: maxBatchDelay);

            var queue = new RegistrationWriteQueue(options);
            var reader = new RegistrationBatchReader(queue, options);
            var cancellationToken = TestContext.Current.CancellationToken;
            var request = RegistrationBatchTestData.CreateRequest();

            var enqueueTask = queue.EnqueueAsync(
                request,
                cancellationToken);

            var stopwatch = Stopwatch.StartNew();

            var batch = await reader.ReadAsync(cancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);

            stopwatch.Stop();

            Assert.Single(batch);
            Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(50));

            CompleteBatch(batch);

            await enqueueTask;
        }

        [Fact]
        public async Task ReadAsync_WhenApplicationIsStopping_ThrowsCancellation()
        {
            var options = RegistrationBatchTestData.CreateOptions(
                maxBatchSize: 10,
                maxBatchDelay: TimeSpan.FromMilliseconds(5));

            var queue = new RegistrationWriteQueue(options);
            var reader = new RegistrationBatchReader(queue, options);

            using var cancellationTokenSource =
                new CancellationTokenSource();

            cancellationTokenSource.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => reader.ReadAsync(cancellationTokenSource.Token));
        }

        private static void CompleteBatch(
            IReadOnlyList<QueuedRegistrationWrite> batch)
        {
            foreach (var queuedWrite in batch)
            {
                queuedWrite.Complete(
                    new RegisterDocumentResponse(
                        queuedWrite.Request.Operation.Id,
                        queuedWrite.Request.Operation.Status));
            }
        }
    }
}
