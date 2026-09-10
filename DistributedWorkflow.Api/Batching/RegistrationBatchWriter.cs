
using DistributedWorkflow.Api.Models;

namespace DistributedWorkflow.Api.Batching
{
    internal sealed class RegistrationBatchWriter : BackgroundService
    {
        private readonly RegistrationBatchReader _batchReader;
        private readonly IRegistrationBatchStore _batchStore;
        private readonly ILogger<RegistrationBatchWriter> _logger;

        public RegistrationBatchWriter(
            RegistrationBatchReader reader,
            IRegistrationBatchStore store,
            ILogger<RegistrationBatchWriter> logger)
        {
            _batchReader = reader;
            _batchStore = store;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(
            CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var batch =
                        await _batchReader.ReadAsync(stoppingToken);

                    await ProcessBatchAsync(
                        batch,
                        stoppingToken);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }

        private async Task ProcessBatchAsync(
            IReadOnlyList<QueuedRegistrationWrite> batch,
            CancellationToken stoppingToken)
        {
            try
            {
                var requests =
                    new List<RegistrationWriteRequest>(batch.Count);

                foreach (var queuedWrite in batch)
                {
                    requests.Add(queuedWrite.Request);
                }

                var results = await _batchStore.WriteAsync(
                    requests,
                    stoppingToken);

                if (results.Count != batch.Count)
                {
                    throw new InvalidOperationException(
                        "Batch store returned an unexpected number of results.");
                }

                for (var index = 0; index < batch.Count; index++)
                {
                    var result = results[index];

                    var response =
                        new RegisterDocumentResponse(
                            result.OperationId,
                            result.Status);

                    batch[index].Complete(response);
                }
            }
            catch (OperationCanceledException exception)
                when (stoppingToken.IsCancellationRequested)
            {
                FailBatch(batch, exception);
                throw;
            }
            catch (Exception exception)
            {
                FailBatch(batch, exception);

                _logger.LogError(
                    exception,
                    "Registration batch write failed. BatchSize={BatchSize}",
                    batch.Count);
            }
        }

        private static void FailBatch(
            IReadOnlyList<QueuedRegistrationWrite> batch,
            Exception exception)
        {
            foreach (var queuedWrite in batch)
            {
                queuedWrite.Fail(exception);
            }
        }
    }
}
