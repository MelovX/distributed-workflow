using DistributedWorkflow.Api.Data;
using DistributedWorkflow.Api.Metrics;
using Microsoft.EntityFrameworkCore;

namespace DistributedWorkflow.Api.Services
{
    public class OutboxDispatcher : BackgroundService
    {
        private const int BatchSize = 100;

        private readonly IServiceScopeFactory _scopeFactory;
        private readonly RabbitMqRegistrationPublisher _publisher;
        private readonly ILogger<OutboxDispatcher> _logger;

        private readonly string _dispatcherId = $"{Environment.MachineName}-{Guid.NewGuid():N}";

        public OutboxDispatcher(
            IServiceScopeFactory scopeFactory,
            RabbitMqRegistrationPublisher publisher,
            ILogger<OutboxDispatcher> logger)
        {
            _scopeFactory = scopeFactory;
            _publisher = publisher;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation(
                "Outbox dispatcher {DispatcherId} started.",
                _dispatcherId);

            while (!stoppingToken.IsCancellationRequested)
            {
                var processedCount = 0;

                try
                {
                    processedCount = await DispatchBatchAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Outbox dispatcher iteration failed.");
                }

                if (processedCount == 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }
        }

        private async Task<int> DispatchBatchAsync(CancellationToken cancellationToken)
        {
            using var scope = _scopeFactory.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<RegistrationDbContext>();

            var now = DateTimeOffset.UtcNow;
            var lockUntil = now.AddSeconds(20);
            var lockOwner = $"{_dispatcherId}-{Guid.NewGuid():N}";

            await using var transaction =
                await dbContext.Database.BeginTransactionAsync(cancellationToken);

            var messages = await dbContext.OutboxMessages
                .FromSqlInterpolated($"""
                    SELECT *
                    FROM "OutboxMessages"
                    WHERE "PublishedAt" IS NULL
                      AND ("LockedUntil" IS NULL OR "LockedUntil" < {now})
                    ORDER BY "CreatedAt", "Id"
                    LIMIT {BatchSize}
                    FOR UPDATE SKIP LOCKED
                    """)
                .ToListAsync(cancellationToken);

            if (messages.Count == 0)
            {
                await transaction.CommitAsync(cancellationToken);
                return 0;
            }

            foreach (var message in messages)
            {
                message.LockedUntil = lockUntil;
                message.LockedBy = lockOwner;
            }

            await dbContext.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            foreach (var message in messages)
            {
                try
                {
                    await _publisher.PublishRawAsync(
                        messageId: message.Id.ToString(),
                        payload: message.Payload,
                        cancellationToken: cancellationToken);

                    message.PublishedAt = DateTimeOffset.UtcNow;
                    message.LockedUntil = null;
                    message.LockedBy = null;
                    message.LastError = null;

                    RegistrationMetrics.OutboxPublished.Add(1);
                }
                catch (Exception ex)
                {
                    message.Attempts += 1;
                    message.LastError = ex.Message;

                    _logger.LogError(
                        ex,
                        "Failed to publish outbox message {MessageId}. Attempt {Attempt}.",
                        message.Id,
                        message.Attempts);

                    RegistrationMetrics.OutboxFailed.Add(1);
                }
            }

            await dbContext.SaveChangesAsync(cancellationToken);

            _logger.LogInformation("Processed {Count} outbox messages.", messages.Count);

            return messages.Count;
        }
    }
}
