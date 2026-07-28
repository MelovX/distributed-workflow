using Interview.Playground.Api.Data;
using Interview.Playground.Api.Data.Entities;
using Interview.Playground.Api.Metrics;
using Interview.Playground.Api.Models;
using Interview.Playground.Api.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;

namespace Interview.Playground.Api.Controllers
{
    [ApiController]
    [Route("registrations")]
    public sealed class RegistrationsController : ControllerBase
    {
        private readonly RegistrationDbContext _dbContext;

        public RegistrationsController(RegistrationDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        [HttpPost]
        public async Task<ActionResult<RegisterDocumentResponse>> RegisterDocument(
            RegisterDocumentRequest request,
            [FromHeader(Name = "Idempotency-Key")] string? idempotencyKey,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(idempotencyKey))
            {
                return BadRequest("Idempotency-Key header is required.");
            }

            var existingOperation = await _dbContext.RegistrationOperations
                .FirstOrDefaultAsync(x => x.IdempotencyKey == idempotencyKey, cancellationToken);

            if (existingOperation is not null)
            {
                return Accepted(new RegisterDocumentResponse(
                    OperationId: existingOperation.Id,
                    Status: existingOperation.Status));
            }

            var now = DateTimeOffset.UtcNow;
            var operationId = Guid.NewGuid().ToString("N");

            var command = new RegisterDocumentCommand(
                OperationId: operationId,
                DocumentId: request.DocumentId,
                Title: request.Title,
                CreatedAt: now);

            var operation = new RegistrationOperation
            {
                Id = operationId,
                IdempotencyKey = idempotencyKey,
                DocumentId = request.DocumentId,
                Title = request.Title,
                Status = "Pending",
                CreatedAt = now
            };

            var outboxMessage = new OutboxMessage
            {
                Id = Guid.NewGuid(),
                Type = nameof(RegisterDocumentCommand),
                Payload = JsonSerializer.Serialize(command),
                CreatedAt = now,
                PublishedAt = null
            };

            await _dbContext.RegistrationOperations.AddAsync(operation, cancellationToken);
            await _dbContext.OutboxMessages.AddAsync(outboxMessage, cancellationToken);

            await _dbContext.SaveChangesAsync(cancellationToken);

            RegistrationMetrics.RegistrationsCreated.Add(1);

            return Accepted(new RegisterDocumentResponse(
                OperationId: command.OperationId,
                Status: "Pending"));
        }
    }
}
