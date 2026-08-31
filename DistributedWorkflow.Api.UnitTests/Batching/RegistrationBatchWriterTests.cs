using DistributedWorkflow.Api.Batching;
using Microsoft.Extensions.Logging.Abstractions;

namespace DistributedWorkflow.Api.UnitTests.Batching
{
    public sealed class RegistrationBatchWriterTests
    {
        [Fact]
        public async Task Writer_WhenBatchIsStored_CompletesEveryRequest()
        {
            var options = RegistrationBatchTestData.CreateOptions(
                maxBatchSize: 2,
                maxBatchDelay: TimeSpan.FromSeconds(1));

            var queue = new RegistrationWriteQueue(options);
            var reader = new RegistrationBatchReader(queue, options);
            var store = new StubRegistrationBatchStore((
                    IReadOnlyList<RegistrationWriteRequest> requests,
                    CancellationToken _) =>
                {
                    IReadOnlyList<RegistrationWriteResult> results = requests
                        .Select(request =>
                            new RegistrationWriteResult(
                                request.Operation.Id,
                                request.Operation.Status))
                        .ToArray();

                    return Task.FromResult(results);
                });

            using var writer = new RegistrationBatchWriter(
                reader,
                store,
                NullLogger<RegistrationBatchWriter>.Instance);

            var cancellationToken = TestContext.Current.CancellationToken;

            await writer.StartAsync(cancellationToken);

            try
            {
                var firstRequest = RegistrationBatchTestData.CreateRequest();
                var secondRequest = RegistrationBatchTestData.CreateRequest();

                var responseTasks = new[]
                {
                    queue.EnqueueAsync(firstRequest, cancellationToken),
                    queue.EnqueueAsync(secondRequest, cancellationToken)
                };

                var responses = await Task.WhenAll(responseTasks)
                    .WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);

                Assert.Equal(firstRequest.Operation.Id, responses[0].OperationId);
                Assert.Equal(secondRequest.Operation.Id, responses[1].OperationId);

                Assert.Equal(1, store.CallCount);
                Assert.NotNull(store.LastRequests);
                Assert.Equal(2, store.LastRequests.Count);
            }
            finally
            {
                await writer.StopAsync(CancellationToken.None);
            }
        }

        [Fact]
        public async Task Writer_WhenStoreFails_FaultsCurrentRequestAndProcessesNextOne()
        {
            var options = RegistrationBatchTestData.CreateOptions(
                maxBatchSize: 1,
                maxBatchDelay: TimeSpan.FromMilliseconds(5));

            var queue = new RegistrationWriteQueue(options);
            var reader = new RegistrationBatchReader(queue, options);
            var firstRequest = RegistrationBatchTestData.CreateRequest();
            var secondRequest = RegistrationBatchTestData.CreateRequest();
            var expectedException = new InvalidOperationException(
                "PostgreSQL write failed.");

            var attempt = 0;

            var store = new StubRegistrationBatchStore((
                IReadOnlyList<RegistrationWriteRequest> _,
                CancellationToken _) =>
                {
                    attempt++;

                    if (attempt == 1)
                    {
                        return Task.FromException<
                            IReadOnlyList<RegistrationWriteResult>>(
                                expectedException);
                    }

                    IReadOnlyList<RegistrationWriteResult> results =
                    [
                        new RegistrationWriteResult(
                            secondRequest.Operation.Id,
                            secondRequest.Operation.Status)
                    ];

                    return Task.FromResult(results);
                });

            using var writer = new RegistrationBatchWriter(
                reader,
                store,
                NullLogger<RegistrationBatchWriter>.Instance);

            var cancellationToken = TestContext.Current.CancellationToken;

            await writer.StartAsync(cancellationToken);

            try
            {
                var failedTask = queue.EnqueueAsync(
                    firstRequest,
                    cancellationToken);

                var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                    () => failedTask.WaitAsync(
                        TimeSpan.FromSeconds(2),
                        cancellationToken));

                Assert.Same(expectedException, exception);

                var response = await queue.EnqueueAsync(
                        secondRequest,
                        cancellationToken)
                    .WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);

                Assert.Equal(secondRequest.Operation.Id, response.OperationId);

                Assert.Equal(2, store.CallCount);
            }
            finally
            {
                await writer.StopAsync(CancellationToken.None);
            }
        }

        [Fact]
        public async Task Writer_WhenStoreReturnsWrongResultCount_FaultsEntireBatch()
        {
            var options = RegistrationBatchTestData.CreateOptions(
                maxBatchSize: 2,
                maxBatchDelay: TimeSpan.FromSeconds(1));

            var queue = new RegistrationWriteQueue(options);
            var reader = new RegistrationBatchReader(queue, options);

            var store = new StubRegistrationBatchStore((
                IReadOnlyList<RegistrationWriteRequest> requests,
                CancellationToken _) =>
                {
                    IReadOnlyList<RegistrationWriteResult> results =
                    [
                        new RegistrationWriteResult(
                            requests[0].Operation.Id,
                            requests[0].Operation.Status)
                    ];

                    return Task.FromResult(results);
                });

            using var writer = new RegistrationBatchWriter(
                reader,
                store,
                NullLogger<RegistrationBatchWriter>.Instance);

            var cancellationToken = TestContext.Current.CancellationToken;

            await writer.StartAsync(cancellationToken);

            try
            {
                var firstTask = queue.EnqueueAsync(
                    RegistrationBatchTestData.CreateRequest(),
                    cancellationToken);

                var secondTask = queue.EnqueueAsync(
                    RegistrationBatchTestData.CreateRequest(),
                    cancellationToken);

                var exception = await Assert.ThrowsAsync<
                    InvalidOperationException>(
                        () => Task.WhenAll(firstTask, secondTask)
                            .WaitAsync(
                                TimeSpan.FromSeconds(2),
                                cancellationToken));

                Assert.Equal(
                    "Batch store returned an unexpected number of results.",
                    exception.Message);
            }
            finally
            {
                await writer.StopAsync(CancellationToken.None);
            }
        }

        private sealed class StubRegistrationBatchStore :
            IRegistrationBatchStore
        {
            private readonly Func<
                IReadOnlyList<RegistrationWriteRequest>,
                CancellationToken,
                Task<IReadOnlyList<RegistrationWriteResult>>> _writeAsync;

            public StubRegistrationBatchStore(
                Func<
                    IReadOnlyList<RegistrationWriteRequest>,
                    CancellationToken,
                    Task<IReadOnlyList<RegistrationWriteResult>>> writeAsync)
            {
                _writeAsync = writeAsync;
            }

            public int CallCount { get; private set; }

            public IReadOnlyList<RegistrationWriteRequest>? LastRequests
            {
                get;
                private set;
            }

            public Task<IReadOnlyList<RegistrationWriteResult>> WriteAsync(
                IReadOnlyList<RegistrationWriteRequest> requests,
                CancellationToken cancellationToken)
            {
                CallCount++;
                LastRequests = requests;

                return _writeAsync(requests, cancellationToken);
            }
        }
    }
}
