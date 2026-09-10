using Confluent.Kafka;
using DistributedWorkflow.RegistrationStatusUpdater.Configuration;
using DistributedWorkflow.RegistrationStatusUpdater.Data;
using DistributedWorkflow.RegistrationStatusUpdater.Events;
using DistributedWorkflow.RegistrationStatusUpdater.Metrics;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace DistributedWorkflow.RegistrationStatusUpdater.Services
{
    public sealed class RegistrationStatusConsumer : BackgroundService
    {
        private readonly int _consumerIndex;
        private readonly KafkaOptions _options;
        private readonly RegistrationStatusStore _statusStore;
        private readonly ILogger<RegistrationStatusConsumer> _logger;

        public RegistrationStatusConsumer(
            int consumerIndex,
            IOptions<KafkaOptions> options,
            RegistrationStatusStore statusStore,
            ILogger<RegistrationStatusConsumer> logger)
        {
            _consumerIndex = consumerIndex;
            _options = options.Value;
            _statusStore = statusStore;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(
            CancellationToken stoppingToken)
        {
            await Task.Yield();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await RunConsumerLoopAsync(stoppingToken);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    StatusUpdaterMetrics.IterationErrors.Add(1);

                    _logger.LogError(
                        exception,
                        "Kafka status consumer failed. ConsumerIndex={ConsumerIndex}. Retrying in {DelaySeconds} seconds.",
                        _consumerIndex,
                        _options.ErrorDelay.TotalSeconds);

                    await Task.Delay(
                        _options.ErrorDelay,
                        stoppingToken);
                }
            }
        }

        private async Task RunConsumerLoopAsync(
            CancellationToken stoppingToken)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                GroupId = _options.GroupId,
                ClientId =
                    $"{Environment.MachineName}-status-updater-{_consumerIndex}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,
                IsolationLevel = IsolationLevel.ReadCommitted
            };

            using var consumer =
                new ConsumerBuilder<string, string>(consumerConfig)
                    .Build();

            consumer.Subscribe(_options.TopicName);

            _logger.LogInformation(
                "Kafka status consumer started. ConsumerIndex={ConsumerIndex}, GroupId={GroupId}, Topic={Topic}.",
                _consumerIndex,
                _options.GroupId,
                _options.TopicName);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var firstMessage = consumer.Consume(stoppingToken);
                    var batch = ConsumeBatch(
                        consumer,
                        firstMessage,
                        stoppingToken);

                    await ProcessBatchAsync(
                        consumer,
                        batch,
                        stoppingToken);
                }
            }
            finally
            {
                consumer.Close();
            }
        }

        private List<ConsumeResult<string, string>> ConsumeBatch(
            IConsumer<string, string> consumer,
            ConsumeResult<string, string> firstMessage,
            CancellationToken stoppingToken)
        {
            var batch =
                new List<ConsumeResult<string, string>>(
                    _options.BatchSize)
                {
                    firstMessage
                };

            var deadline = DateTime.UtcNow + _options.BatchDelay;

            while (batch.Count < _options.BatchSize)
            {
                stoppingToken.ThrowIfCancellationRequested();

                var remaining = deadline - DateTime.UtcNow;

                if (remaining <= TimeSpan.Zero)
                {
                    break;
                }

                var message = consumer.Consume(remaining);

                if (message is null)
                {
                    break;
                }

                batch.Add(message);
            }

            return batch;
        }

        private async Task ProcessBatchAsync(
            IConsumer<string, string> consumer,
            IReadOnlyList<ConsumeResult<string, string>> batch,
            CancellationToken cancellationToken)
        {
            var startedAt = Stopwatch.GetTimestamp();
            var operationIds = new List<string>(batch.Count);

            try
            {
                foreach (var message in batch)
                {
                    if (TryParseEvent(
                        message.Message.Value,
                        out var documentEvent,
                        out var error))
                    {
                        operationIds.Add(documentEvent.OperationId);
                        continue;
                    }

                    StatusUpdaterMetrics.InvalidEvents.Add(1);

                    _logger.LogError(
                        "Invalid DocumentRegisteredEvent skipped. Topic={Topic}, Partition={Partition}, Offset={Offset}, Error={ParsingError}.",
                        message.Topic,
                        message.Partition.Value,
                        message.Offset.Value,
                        error);
                }

                var updatedCount =
                    await _statusStore.MarkSucceededAsync(
                        operationIds,
                        cancellationToken);

                var offsets = batch
                    .GroupBy(message => message.TopicPartition)
                    .Select(group =>
                        new TopicPartitionOffset(
                            group.Key,
                            new Offset(
                                group.Max(message =>
                                    message.Offset.Value) + 1)))
                    .ToArray();

                consumer.Commit(offsets);

                StatusUpdaterMetrics.EventsConsumed.Add(batch.Count);
                StatusUpdaterMetrics.OperationsUpdated.Add(updatedCount);
                StatusUpdaterMetrics.OperationsUnchanged.Add(
                    operationIds.Distinct().Count() - updatedCount);
                StatusUpdaterMetrics.BatchSize.Record(batch.Count);

                _logger.LogDebug(
                    "Registration status batch completed. ConsumerIndex={ConsumerIndex}, Consumed={ConsumedCount}, Updated={UpdatedCount}.",
                    _consumerIndex,
                    batch.Count,
                    updatedCount);
            }
            finally
            {
                StatusUpdaterMetrics.BatchDuration.Record(
                    Stopwatch.GetElapsedTime(startedAt).TotalSeconds);
            }
        }

        private static bool TryParseEvent(
            string? payload,
            [NotNullWhen(true)] out DocumentRegisteredEvent? documentEvent,
            out string error)
        {
            if (string.IsNullOrWhiteSpace(payload))
            {
                documentEvent = null;
                error = "Payload is empty.";

                return false;
            }

            try
            {
                documentEvent =
                    JsonSerializer.Deserialize<DocumentRegisteredEvent>(
                        payload);
            }
            catch (JsonException exception)
            {
                documentEvent = null;
                error = exception.Message;

                return false;
            }

            if (documentEvent is null)
            {
                error = "Payload contains JSON null.";

                return false;
            }

            if (string.IsNullOrWhiteSpace(documentEvent.EventId) ||
                string.IsNullOrWhiteSpace(documentEvent.OperationId) ||
                string.IsNullOrWhiteSpace(documentEvent.DocumentId) ||
                string.IsNullOrWhiteSpace(documentEvent.Title) ||
                documentEvent.RegisteredAt == default)
            {
                documentEvent = null;
                error = "Event contains invalid required fields.";

                return false;
            }

            error = string.Empty;

            return true;
        }
    }
}
