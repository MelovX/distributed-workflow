using Confluent.Kafka;
using Interview.Playground.Worker.Events;
using System.Text.Json;

namespace Interview.Playground.Worker.Services
{
    public class KafkaDocumentEventPublisher : IDisposable
    {
        private const string TopicName = "document-registered";

        private readonly IProducer<string, string> _producer;

        public KafkaDocumentEventPublisher()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:19092",
                Acks = Acks.All,
                EnableIdempotence = true
            };

            _producer = new ProducerBuilder<string, string>(config).Build();
        }

        public async Task PublishAsync(
            DocumentRegisteredEvent @event,
            CancellationToken cancellationToken = default)
        {
            var json = JsonSerializer.Serialize(@event);

            var message = new Message<string, string>
            {
                Key = @event.DocumentId,
                Value = json,
                Headers =
                [
                    new Header("event-type", "DocumentRegisteredEvent"u8.ToArray()),
                    new Header("operation-id", System.Text.Encoding.UTF8.GetBytes(@event.OperationId))
                ]
            };

            await _producer.ProduceAsync(TopicName, message, cancellationToken);
        }

        public void Dispose()
        {
            _producer.Flush(TimeSpan.FromSeconds(5));
            _producer.Dispose();
        }
    }
}
