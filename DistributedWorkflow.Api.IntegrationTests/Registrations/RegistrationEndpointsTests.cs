using DistributedWorkflow.Api.Data;
using DistributedWorkflow.Api.Data.Entities;
using DistributedWorkflow.Api.IntegrationTests.Factories;
using DistributedWorkflow.Api.IntegrationTests.Fixtures;
using DistributedWorkflow.Api.Models;
using Microsoft.EntityFrameworkCore;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace DistributedWorkflow.Api.IntegrationTests.Registrations
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
                _fixture.Container.GetConnectionString());

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

        [Fact]
        public async Task Register_WhenIdempotencyKeyIsRepeated_ReturnsSameOperationAndDoesNotCreateDuplicates()
        {
            // Arrange
            var documentId = "document-123";
            var title = "Test document";
            var idempotencyKey = Guid.NewGuid().ToString("N");

            using var factory = new ApiWebApplicationFactory(
                _fixture.Container.GetConnectionString());

            using var client = factory.CreateClient();

            var request = new RegisterDocumentRequest(
                DocumentId: documentId,
                Title: title);

            using var firstHttpRequest = new HttpRequestMessage(
                HttpMethod.Post,
                "/registrations");

            firstHttpRequest.Content = JsonContent.Create(request);
            firstHttpRequest.Headers.Add("Idempotency-Key", idempotencyKey);

            using var secondHttpRequest = new HttpRequestMessage(
                HttpMethod.Post,
                "/registrations");

            secondHttpRequest.Content = JsonContent.Create(request);
            secondHttpRequest.Headers.Add("Idempotency-Key", idempotencyKey);

            // Act
            using var firstResponse = await client.SendAsync(firstHttpRequest,
                TestContext.Current.CancellationToken);

            using var secondResponse = await client.SendAsync(secondHttpRequest,
                TestContext.Current.CancellationToken);

            // Assert
            Assert.Equal(HttpStatusCode.Accepted, firstResponse.StatusCode);
            Assert.Equal(HttpStatusCode.Accepted, secondResponse.StatusCode);

            var firstContent = await firstResponse.Content.ReadFromJsonAsync<RegisterDocumentResponse>(cancellationToken:
                TestContext.Current.CancellationToken);

            var secondContent = await secondResponse.Content.ReadFromJsonAsync<RegisterDocumentResponse>(cancellationToken:
                TestContext.Current.CancellationToken);

            // Assert
            Assert.NotNull(firstContent);
            Assert.NotNull(secondContent);
            Assert.NotEmpty(firstContent.OperationId);
            Assert.NotEmpty(secondContent.OperationId);
            Assert.Equal("Pending", firstContent.Status);
            Assert.Equal("Pending", secondContent.Status);

            Assert.Equal(firstContent.OperationId, secondContent.OperationId);

            var options = new DbContextOptionsBuilder<RegistrationDbContext>()
                .UseNpgsql(_fixture.Container.GetConnectionString())
                .Options;

            await using var dbContext = new RegistrationDbContext(options);

            var operations = await dbContext.RegistrationOperations
                .AsNoTracking()
                .Where(operation =>
                    operation.IdempotencyKey == idempotencyKey)
                .ToListAsync(TestContext.Current.CancellationToken);

            var operation = Assert.Single(operations);

            Assert.Equal(firstContent.OperationId, operation.Id);

            var outboxMessages = await dbContext.OutboxMessages
                .AsNoTracking()
                .Where(message =>
                    message.Type == nameof(RegisterDocumentCommand))
                .ToListAsync(TestContext.Current.CancellationToken);

            var matchingOutboxMessages = outboxMessages.Where(message =>
            {
                var outboxCommand = JsonSerializer.Deserialize<RegisterDocumentCommand>(message.Payload);

                return outboxCommand?.OperationId == firstContent.OperationId;
            });

            Assert.Single(matchingOutboxMessages);
        }

        [Fact]
        public async Task Register_WhenIdempotencyKeyIsSubmittedConcurrently_DoesNotCreateDuplicates()
        {
            // Arrange
            var documentId = "document-123";
            var title = "Test document";
            var idempotencyKey = Guid.NewGuid().ToString("N");

            using var factory = new ApiWebApplicationFactory(
                _fixture.Container.GetConnectionString());

            using var client = factory.CreateClient();

            var request = new RegisterDocumentRequest(
                DocumentId: documentId,
                Title: title);

            using var firstHttpRequest = new HttpRequestMessage(
                HttpMethod.Post,
                "/registrations");

            firstHttpRequest.Content = JsonContent.Create(request);
            firstHttpRequest.Headers.Add("Idempotency-Key", idempotencyKey);

            using var secondHttpRequest = new HttpRequestMessage(
                HttpMethod.Post,
                "/registrations");

            secondHttpRequest.Content = JsonContent.Create(request);
            secondHttpRequest.Headers.Add("Idempotency-Key", idempotencyKey);

            // Act
            var firstResponseTask = client.SendAsync(firstHttpRequest,
                TestContext.Current.CancellationToken);

            var secondResponseTask = client.SendAsync(secondHttpRequest,
                TestContext.Current.CancellationToken);

            var responses = await Task.WhenAll(
                firstResponseTask,
                secondResponseTask);

            using var firstResponse = responses[0];
            using var secondResponse = responses[1];

            // Assert
            Assert.Equal(HttpStatusCode.Accepted, firstResponse.StatusCode);
            Assert.Equal(HttpStatusCode.Accepted, secondResponse.StatusCode);

            var firstContent = await firstResponse.Content.ReadFromJsonAsync<RegisterDocumentResponse>(cancellationToken:
                TestContext.Current.CancellationToken);

            var secondContent = await secondResponse.Content.ReadFromJsonAsync<RegisterDocumentResponse>(cancellationToken:
                TestContext.Current.CancellationToken);

            // Assert
            Assert.NotNull(firstContent);
            Assert.NotNull(secondContent);
            Assert.NotEmpty(firstContent.OperationId);
            Assert.NotEmpty(secondContent.OperationId);
            Assert.Equal("Pending", firstContent.Status);
            Assert.Equal("Pending", secondContent.Status);

            Assert.Equal(firstContent.OperationId, secondContent.OperationId);

            var options = new DbContextOptionsBuilder<RegistrationDbContext>()
                .UseNpgsql(_fixture.Container.GetConnectionString())
                .Options;

            await using var dbContext = new RegistrationDbContext(options);

            var operations = await dbContext.RegistrationOperations
                .AsNoTracking()
                .Where(operation =>
                    operation.IdempotencyKey == idempotencyKey)
                .ToListAsync(TestContext.Current.CancellationToken);

            var operation = Assert.Single(operations);

            Assert.Equal(firstContent.OperationId, operation.Id);

            var outboxMessages = await dbContext.OutboxMessages
                .AsNoTracking()
                .Where(message =>
                    message.Type == nameof(RegisterDocumentCommand))
                .ToListAsync(TestContext.Current.CancellationToken);

            var matchingOutboxMessages = outboxMessages.Where(message =>
            {
                var outboxCommand = JsonSerializer.Deserialize<RegisterDocumentCommand>(message.Payload);

                return outboxCommand?.OperationId == firstContent.OperationId;
            });

            Assert.Single(matchingOutboxMessages);
        }

        [Fact]
        public async Task Register_WhenDistinctRequestsAreSubmittedConcurrently_PersistsEntireBatch()
        {
            const int requestCount = 16;

            using var factory = new ApiWebApplicationFactory(
                _fixture.Container.GetConnectionString());

            using var client = factory.CreateClient();

            var idempotencyKeys = Enumerable.Range(0, requestCount)
                .Select(_ => Guid.NewGuid().ToString("N"))
                .ToArray();

            var httpRequests = idempotencyKeys
                .Select((idempotencyKey, index) =>
                {
                    var httpRequest = new HttpRequestMessage(
                        HttpMethod.Post,
                        "/registrations");

                    httpRequest.Content = JsonContent.Create(
                        new RegisterDocumentRequest(
                            $"batch-document-{index}",
                            $"Batch document {index}"));

                    httpRequest.Headers.Add(
                        "Idempotency-Key",
                        idempotencyKey);

                    return httpRequest;
                })
                .ToArray();

            HttpResponseMessage[]? responses = null;

            try
            {
                responses = await Task.WhenAll(
                    httpRequests.Select(httpRequest =>
                        client.SendAsync(
                            httpRequest,
                            TestContext.Current.CancellationToken)));

                Assert.All(
                    responses,
                    response => Assert.Equal(
                        HttpStatusCode.Accepted,
                        response.StatusCode));

                var responseBodies = await Task.WhenAll(
                    responses.Select(response =>
                        response.Content.ReadFromJsonAsync<
                            RegisterDocumentResponse>(
                                cancellationToken:
                                    TestContext.Current.CancellationToken)));

                Assert.All(responseBodies, Assert.NotNull);

                var operationIds = responseBodies
                    .Select(response => response!.OperationId)
                    .ToHashSet(StringComparer.Ordinal);

                Assert.Equal(requestCount, operationIds.Count);

                var options =
                    new DbContextOptionsBuilder<RegistrationDbContext>()
                        .UseNpgsql(
                            _fixture.Container.GetConnectionString())
                        .Options;

                await using var dbContext =
                    new RegistrationDbContext(options);

                var operations = await dbContext.RegistrationOperations
                    .AsNoTracking()
                    .Where(operation =>
                        idempotencyKeys.Contains(
                            operation.IdempotencyKey))
                    .ToListAsync(TestContext.Current.CancellationToken);

                Assert.Equal(requestCount, operations.Count);

                var outboxMessages = await dbContext.OutboxMessages
                    .AsNoTracking()
                    .Where(message =>
                        message.Type == nameof(RegisterDocumentCommand))
                    .ToListAsync(TestContext.Current.CancellationToken);

                var matchingOutboxCount = outboxMessages.Count(message =>
                {
                    var command =
                        JsonSerializer.Deserialize<RegisterDocumentCommand>(
                            message.Payload);

                    return command is not null
                           && operationIds.Contains(command.OperationId);
                });

                Assert.Equal(requestCount, matchingOutboxCount);
            }
            finally
            {
                foreach (var httpRequest in httpRequests)
                {
                    httpRequest.Dispose();
                }

                if (responses is not null)
                {
                    foreach (var response in responses)
                    {
                        response.Dispose();
                    }
                }
            }
        }

        [Fact]
        public async Task GetStatus_WhenOperationExists_ReturnsOkWithOperationStatus()
        {
            // Arrange
            using var factory = new ApiWebApplicationFactory(
                _fixture.Container.GetConnectionString());

            using var client = factory.CreateClient();

            var options = new DbContextOptionsBuilder<RegistrationDbContext>()
                .UseNpgsql(_fixture.Container.GetConnectionString())
                .Options;

            await using var dbContext = new RegistrationDbContext(options);

            var operationId = Guid.NewGuid();
            var registrationOperation = new RegistrationOperation()
            {
                Id = operationId.ToString(),
                IdempotencyKey = Guid.NewGuid().ToString(),
                DocumentId = Guid.NewGuid().ToString(),
                CreatedAt = DateTime.UtcNow,
                Status = "Succeeded",
                Title = "Test"
            };

            await dbContext.RegistrationOperations.AddAsync(registrationOperation, TestContext.Current.CancellationToken);

            await dbContext.SaveChangesAsync(TestContext.Current.CancellationToken);

            // Act
            using var response = await client.GetAsync($"/registrations/{operationId}",
                TestContext.Current.CancellationToken);

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var content = await response.Content.ReadFromJsonAsync<RegisterDocumentResponse>(
                    cancellationToken: TestContext.Current.CancellationToken);

            // Assert
            Assert.NotNull(content);
            Assert.Equal(operationId.ToString(), content.OperationId);
            Assert.Equal("Succeeded", content.Status);
        }

        [Fact]
        public async Task GetStatus_WhenOperationDoesNotExist_ReturnsNotFound()
        {
            // Arrange
            using var factory = new ApiWebApplicationFactory(
                _fixture.Container.GetConnectionString());

            using var client = factory.CreateClient();

            // Act
            using var response = await client.GetAsync($"/registrations/{Guid.NewGuid()}",
                TestContext.Current.CancellationToken);

            // Assert
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        }
    }
}
