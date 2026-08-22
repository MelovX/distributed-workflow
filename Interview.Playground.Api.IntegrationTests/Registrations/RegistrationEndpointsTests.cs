using Interview.Playground.Api.Data;
using Interview.Playground.Api.IntegrationTests.Factories;
using Interview.Playground.Api.IntegrationTests.Fixtures;
using Interview.Playground.Api.Models;
using Microsoft.EntityFrameworkCore;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace Interview.Playground.Api.IntegrationTests.Registrations
{
    public sealed class RegistrationEndpointsTests : IClassFixture<PostgresContainerFixture>
    {
        private readonly PostgresContainerFixture _fixture;

        public RegistrationEndpointsTests(PostgresContainerFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task Register_WhenRequestIsValid_ReturnsAcceptedAndPersistsOperationWithOutboxMessage()
        {
            // Arrange
            var documentId = "document-123";
            var title = "Test document";
            var idempotencyKey = Guid.NewGuid().ToString("N");

            using var factory = new ApiWebApplicationFactory(
                _fixture.Container.GetConnectionString(),
                "localhost:1,abortConnect=false");

            using var client = factory.CreateClient();

            var request = new RegisterDocumentRequest(
                DocumentId: documentId,
                Title: title);

            using var httpRequest = new HttpRequestMessage(
                HttpMethod.Post,
                "/registrations");

            httpRequest.Content = JsonContent.Create(request);
            httpRequest.Headers.Add("Idempotency-Key", idempotencyKey);

            // Act
            using var response = await client.SendAsync(httpRequest,
                TestContext.Current.CancellationToken);

            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

            var content = await response.Content.ReadFromJsonAsync<RegisterDocumentResponse>(cancellationToken:
                TestContext.Current.CancellationToken);

            // Assert
            // #1. HTTP-response Check
            Assert.NotNull(content);
            Assert.NotEmpty(content.OperationId);
            Assert.Equal("Pending", content.Status);

            var options = new DbContextOptionsBuilder<RegistrationDbContext>()
                .UseNpgsql(_fixture.Container.GetConnectionString())
                .Options;

            await using var dbContext = new RegistrationDbContext(options);

            var operation = await dbContext.RegistrationOperations
                .AsNoTracking()
                .SingleAsync(
                    operation => operation.Id == content.OperationId,
                    TestContext.Current.CancellationToken);

            // #2. Operation check
            Assert.Equal(idempotencyKey, operation.IdempotencyKey);
            Assert.Equal(documentId, operation.DocumentId);
            Assert.Equal(title, operation.Title);
            Assert.Equal("Pending", operation.Status);

            var outboxMessages = await dbContext.OutboxMessages
                .AsNoTracking()
                .Where(message =>
                    message.Type == nameof(RegisterDocumentCommand))
                .ToListAsync(TestContext.Current.CancellationToken);

            var outboxMessage = outboxMessages.Single(m =>
            {
                var command = JsonSerializer.Deserialize<RegisterDocumentCommand>(m.Payload);

                return command?.OperationId == content.OperationId;
            });

            var outboxCommand =
                JsonSerializer.Deserialize<RegisterDocumentCommand>(
                    outboxMessage.Payload);

            // #3. Outbox command check
            Assert.NotNull(outboxCommand);
            Assert.Equal(content.OperationId, outboxCommand.OperationId);
            Assert.Equal(documentId, outboxCommand.DocumentId);
            Assert.Equal(title, outboxCommand.Title);

            // #4. Outbox message check
            Assert.Equal(nameof(RegisterDocumentCommand), outboxMessage.Type);
            Assert.Null(outboxMessage.PublishedAt);
            Assert.Equal(0, outboxMessage.Attempts);
        }
    }
}
