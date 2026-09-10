using Confluent.Kafka;
using DistributedWorkflow.KafkaOutboxPublisher.Configuration;
using DistributedWorkflow.KafkaOutboxPublisher.Entities;
using Microsoft.Extensions.Options;
using System.Text;

namespace DistributedWorkflow.KafkaOutboxPublisher.Services
{
    public sealed class KafkaBatchPublisher : IDisposable
    {
        private readonly string _topicName;
        private readonly IProducer<string, string> _producer;

        public KafkaBatchPublisher(IOptions<KafkaOptions> options)
        {
            var kafkaOptions = options.Value;

            _topicName = kafkaOptions.TopicName;

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = kafkaOptions.BootstrapServers,
                ClientId =
                    $"{Environment.MachineName}-kafka-outbox-publisher",
                Acks = Acks.All,
                EnableIdempotence = true,
                CompressionType = CompressionType.Lz4,
                LingerMs = 5,
                MessageTimeoutMs = 10_000
            };

            _producer =
                new ProducerBuilder<string, string>(producerConfig)
                    .Build();
        }

        public async Task<IReadOnlyList<KafkaOutboxPublishResult>> PublishBatchAsync(
                IReadOnlyList<KafkaOutboxPublishRequest> messages,
                CancellationToken cancellationToken)
        {
            var publishTasks =
                new Task<KafkaOutboxPublishResult>[messages.Count];

            for (var index = 0; index < messages.Count; index++)
            {
                publishTasks[index] =
                    PublishOneAsync(
                        messages[index],
                        cancellationToken);
            }

            return await Task.WhenAll(publishTasks);
        }

        private async Task<KafkaOutboxPublishResult> PublishOneAsync(
            KafkaOutboxPublishRequest request,
            CancellationToken cancellationToken)
        {
            try
            {
                var deliveryResult = await _producer.ProduceAsync(
                    _topicName,
                    new Message<string, string>
                    {
                        Key = request.PartitionKey,
                        Value = request.Payload,
                        Headers =
                        [
                            new Header(
                                "event-type",
                                Encoding.UTF8.GetBytes(
                                    request.MessageType)),
                            new Header(
                                "operation-id",
                                Encoding.UTF8.GetBytes(
                                    request.OperationId))
                        ]
                    },
                    cancellationToken);

                if (deliveryResult.Status != PersistenceStatus.Persisted)
                {
                    return new KafkaOutboxPublishResult(
                        request.MessageId,
                        false,
                        new InvalidOperationException(
                            $"Kafka persistence status is " +
                            $"{deliveryResult.Status}."));
                }

                return new KafkaOutboxPublishResult(
                    request.MessageId,
                    true,
                    null);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                return new KafkaOutboxPublishResult(
                    request.MessageId,
                    false,
                    exception);
            }
        }

        public void Dispose()
        {
            _producer.Flush(TimeSpan.FromSeconds(10));
            _producer.Dispose();
        }
    }
}
