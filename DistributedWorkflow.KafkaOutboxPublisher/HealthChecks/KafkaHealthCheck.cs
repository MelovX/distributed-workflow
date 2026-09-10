using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DistributedWorkflow.KafkaOutboxPublisher.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace DistributedWorkflow.KafkaOutboxPublisher.HealthChecks
{
    public sealed class KafkaHealthCheck : IHealthCheck
    {
        private static readonly TimeSpan RequestTimeout =
            TimeSpan.FromSeconds(3);

        private readonly IAdminClient _adminClient;
        private readonly string _topicName;

        public KafkaHealthCheck(
            IAdminClient adminClient,
            IOptions<KafkaOptions> options)
        {
            _adminClient = adminClient;
            _topicName = options.Value.TopicName;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(
            HealthCheckContext context,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var result =
                    await _adminClient.DescribeTopicsAsync(
                        TopicCollection.OfTopicNames([_topicName]),
                        new DescribeTopicsOptions
                        {
                            RequestTimeout = RequestTimeout
                        })
                    .WaitAsync(cancellationToken);

                var topic = result.TopicDescriptions.SingleOrDefault();

                return topic is not null &&
                       topic.Partitions.Count > 0
                    ? HealthCheckResult.Healthy()
                    : HealthCheckResult.Unhealthy(
                        "Kafka topic has no available partitions.");
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                return HealthCheckResult.Unhealthy(
                    exception.Message,
                    exception);
            }
        }
    }
}