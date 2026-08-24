using DistributedWorkflow.Api.Data;
using DistributedWorkflow.Api.Data.Entities;
using DistributedWorkflow.Api.Metrics;
using DistributedWorkflow.Api.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using System.Text.Json;

namespace DistributedWorkflow.Api.Controllers
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

            try
            {
                await _dbContext.SaveChangesAsync(cancellationToken);
            }
            catch (DbUpdateException exception)
                when (exception.InnerException is PostgresException postgresException
                      && postgresException.SqlState ==
                         PostgresErrorCodes.UniqueViolation
                      && postgresException.ConstraintName ==
                         "IX_RegistrationOperations_IdempotencyKey")
            {
                _dbContext.ChangeTracker.Clear();

                var concurrentOperation =
                    await _dbContext.RegistrationOperations
                        .AsNoTracking()
                        .SingleAsync(
                            operation =>
                                operation.IdempotencyKey == idempotencyKey,
                            cancellationToken);

                return Accepted(new RegisterDocumentResponse(
                    OperationId: concurrentOperation.Id,
                    Status: concurrentOperation.Status));
            }

            RegistrationMetrics.RegistrationsCreated.Add(1);

            return Accepted(new RegisterDocumentResponse(
                OperationId: command.OperationId,
                Status: "Pending"));
        }

        [HttpGet("{operationId}")]
        public async Task<ActionResult<RegisterDocumentResponse>> GetStatus(string operationId,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(operationId))
            {
                return BadRequest("operationId is required.");
            }

            var operation = await _dbContext.RegistrationOperations
                                    .AsNoTracking()
                                    .SingleOrDefaultAsync(
                                        operation =>
                                            operation.Id == operationId,
                                        cancellationToken);

            if (operation is null)
            {
                return NotFound();
            }

            return Ok(new RegisterDocumentResponse(
                OperationId: operation.Id,
                Status: operation.Status));
        }
    }
}
