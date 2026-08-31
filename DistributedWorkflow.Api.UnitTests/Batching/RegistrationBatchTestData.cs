using DistributedWorkflow.Api.Batching;
using Microsoft.Extensions.Options;

namespace DistributedWorkflow.Api.UnitTests.Batching
{
    internal static class RegistrationBatchTestData
    {
        public static IOptions<RegistrationBatchOptions> CreateOptions(
            int maxBatchSize,
            TimeSpan maxBatchDelay,
            int capacity = 100)
        {
            return Options.Create(
                new RegistrationBatchOptions
                {
                    MaxBatchSize = maxBatchSize,
                    MaxBatchDelay = maxBatchDelay,
                    Capacity = capacity
                });
        }

        public static RegistrationWriteRequest CreateRequest()
        {
            var operationId = Guid.NewGuid().ToString("N");
            var createdAt = DateTimeOffset.UtcNow;

            return new RegistrationWriteRequest(
                new RegistrationOperationWriteModel(
                    operationId,
                    Guid.NewGuid().ToString("N"),
                    $"document-{operationId}",
                    "Test document",
                    "Pending",
                    createdAt),
                new OutboxMessageWriteModel(
                    Guid.NewGuid(),
                    "RegisterDocumentCommand",
                    "{}",
                    createdAt));
        }
    }
}
