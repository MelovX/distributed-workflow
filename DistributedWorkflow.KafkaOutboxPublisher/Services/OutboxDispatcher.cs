using DistributedWorkflow.KafkaOutboxPublisher.Configuration;
using DistributedWorkflow.KafkaOutboxPublisher.Data;
using DistributedWorkflow.KafkaOutboxPublisher.Metrics;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace DistributedWorkflow.KafkaOutboxPublisher.Services
{
    public sealed class OutboxDispatcher : BackgroundService
    {
        private readonly KafkaOutboxStore _store;
        private readonly KafkaBatchPublisher _publisher;
        private readonly ILogger<OutboxDispatcher> _logger;
        private readonly TimeSpan _emptyBatchDelay;
        private readonly TimeSpan _errorDelay;

        public OutboxDispatcher(
            KafkaOutboxStore store,
            KafkaBatchPublisher publisher,
            IOptions<OutboxOptions> options,
            ILogger<OutboxDispatcher> logger)
        {
            _store = store;
            _publisher = publisher;
            _logger = logger;
            _emptyBatchDelay = options.Value.EmptyBatchDelay;
            _errorDelay = options.Value.ErrorDelay;
        }

        protected override async Task ExecuteAsync(
            CancellationToken stoppingToken)
        {
            _logger.LogInformation(
                "Kafka outbox dispatcher started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var processedCount =
                        await DispatchBatchAsync(stoppingToken);

                    if (processedCount == 0)
                    {
                        await Task.Delay(
                            _emptyBatchDelay,
                            stoppingToken);
                    }
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    KafkaOutboxMetrics.IterationErrors.Add(1);

                    _logger.LogError(
                        exception,
                        "Kafka outbox dispatcher iteration failed.");

                    await Task.Delay(
                        _errorDelay,
                        stoppingToken);
                }
            }

            _logger.LogInformation(
                "Kafka outbox dispatcher stopped.");
        }

        private async Task<int> DispatchBatchAsync(
            CancellationToken cancellationToken)
        {
            var startedAt = Stopwatch.GetTimestamp();

            var batch =
                await _store.ClaimBatchAsync(cancellationToken);

            if (batch.Messages.Count == 0)
            {
                return 0;
            }

            KafkaOutboxMetrics.BatchSize.Record(
                batch.Messages.Count);

            try
            {
                var publishResults =
                    await _publisher.PublishBatchAsync(
                        batch.Messages,
                        cancellationToken);

                var completion =
                    await _store.CompleteBatchAsync(
                        batch.LockOwner,
                        publishResults,
                        cancellationToken);

                KafkaOutboxMetrics.Published.Add(
                    completion.PublishedCount);
                KafkaOutboxMetrics.Failed.Add(
                    completion.FailedCount);
                KafkaOutboxMetrics.OwnershipConflicts.Add(
                    completion.OwnershipConflictCount);

                if (completion.FailedCount > 0)
                {
                    _logger.LogWarning(
                        "Kafka outbox batch contains failed publications. " +
                        "Claimed={ClaimedCount}, Failed={FailedCount}.",
                        batch.Messages.Count,
                        completion.FailedCount);
                }

                if (completion.OwnershipConflictCount > 0)
                {
                    _logger.LogWarning(
                        "Kafka outbox lease ownership conflicts detected. " +
                        "Count={OwnershipConflictCount}.",
                        completion.OwnershipConflictCount);
                }

                return batch.Messages.Count;
            }
            finally
            {
                KafkaOutboxMetrics.BatchDuration.Record(
                    Stopwatch.GetElapsedTime(startedAt).TotalSeconds);
            }
        }
    }
}
