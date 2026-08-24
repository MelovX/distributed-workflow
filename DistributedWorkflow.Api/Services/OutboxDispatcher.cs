using DistributedWorkflow.Api.Data;
using DistributedWorkflow.Api.Metrics;
using Microsoft.EntityFrameworkCore;

namespace DistributedWorkflow.Api.Services
{
    public class OutboxDispatcher : BackgroundService
    {
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
            _logger.LogInformation("Outbox dispatcher started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await DispatchBatchAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Outbox dispatcher iteration failed.");
                }

                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        private async Task DispatchBatchAsync(CancellationToken cancellationToken)
        {
            using var scope = _scopeFactory.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<RegistrationDbContext>();

            var now = DateTimeOffset.UtcNow;
            var lockUntil = now.AddSeconds(20);
            var lockOwner = $"{_dispatcherId}-{Guid.NewGuid():N}";

            var candidateIds = await dbContext.OutboxMessages
                .Where(x =>
                    x.PublishedAt == null &&
                    (x.LockedUntil == null || x.LockedUntil < now))
                .OrderBy(x => x.CreatedAt)
                .Take(10)
                .Select(x => x.Id)
                .ToListAsync(cancellationToken);

            if (candidateIds.Count == 0)
            {
                return;
            }

            await dbContext.OutboxMessages
                .Where(x =>
                    candidateIds.Contains(x.Id) &&
                    x.PublishedAt == null &&
                    (x.LockedUntil == null || x.LockedUntil < now))
                .ExecuteUpdateAsync(setters => setters
                    .SetProperty(x => x.LockedUntil, lockUntil)
                    .SetProperty(x => x.LockedBy, lockOwner),
                    cancellationToken);

            var messages = await dbContext.OutboxMessages
                .Where(x =>
                    x.PublishedAt == null &&
                    x.LockedBy == lockOwner)
                .OrderBy(x => x.CreatedAt)
                .ToListAsync(cancellationToken);

            if (messages.Count == 0)
            {
                return;
            }

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
        }
    }
}
