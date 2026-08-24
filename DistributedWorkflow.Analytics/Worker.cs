using Confluent.Kafka;
using DistributedWorkflow.Analytics.Configuration;
using Microsoft.Extensions.Options;

namespace DistributedWorkflow.Analytics;

public sealed class Worker : BackgroundService
{
    private readonly KafkaOptions _kafkaOptions;

    public Worker(IOptions<KafkaOptions> options)
    {
        _kafkaOptions = options.Value;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => Consume(stoppingToken), stoppingToken);
    }

    private void Consume(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();

        consumer.Subscribe(_kafkaOptions.TopicName);

        Console.WriteLine("Analytics consumer started.");
        Console.WriteLine($"Topic: {_kafkaOptions.TopicName}");
        Console.WriteLine($"GroupId: {config.GroupId}");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);

                Console.WriteLine();
                Console.WriteLine("DocumentRegisteredEvent received");
                Console.WriteLine($"Partition: {result.Partition.Value}");
                Console.WriteLine($"Offset: {result.Offset.Value}");
                Console.WriteLine($"Key: {result.Message.Key}");
                Console.WriteLine($"Value: {result.Message.Value}");

                Console.WriteLine("Analytics processing started...");

                Thread.Sleep(TimeSpan.FromSeconds(1));

                Console.WriteLine("Analytics processing completed.");

                consumer.Commit(result);

                Console.WriteLine(
                    $"Offset committed. Partition={result.Partition.Value}, Offset={result.Offset.Value}");
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Analytics consumer stopping...");
        }
        finally
        {
            consumer.Close();
        }
    }
}