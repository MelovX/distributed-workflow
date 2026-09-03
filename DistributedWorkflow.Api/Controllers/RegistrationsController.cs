using DistributedWorkflow.Api.Batching;
using DistributedWorkflow.Api.Data;
using DistributedWorkflow.Api.Models;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace DistributedWorkflow.Api.Controllers
{
    [ApiController]
    [Route("registrations")]
    public sealed class RegistrationsController : ControllerBase
    {
        private readonly RegistrationQueryStore _queryStore;
        private readonly IRegistrationWriteQueue _registrationWriteQueue;

        public RegistrationsController(
            RegistrationQueryStore queryStore,
            IRegistrationWriteQueue registrationWriteQueue)
        {
            _queryStore = queryStore;
            _registrationWriteQueue = registrationWriteQueue;
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

            var now = DateTimeOffset.UtcNow;
            var operationId = Guid.NewGuid().ToString("N");

            var command = new RegisterDocumentCommand(
                OperationId: operationId,
                DocumentId: request.DocumentId,
                Title: request.Title,
                CreatedAt: now);

            var writeRequest =
                new RegistrationWriteRequest(
                    new RegistrationOperationWriteModel(
                        operationId,
                        idempotencyKey,
                        request.DocumentId,
                        request.Title,
                        "Pending",
                        now),
                    new OutboxMessageWriteModel(
                        Guid.NewGuid(),
                        nameof(RegisterDocumentCommand),
                        JsonSerializer.Serialize(command),
                        now));

            var response =
                await _registrationWriteQueue.EnqueueAsync(
                    writeRequest,
                    cancellationToken);

            return Accepted(response);
        }

        [HttpGet("{operationId}")]
        public async Task<ActionResult<RegisterDocumentResponse>> GetStatus(string operationId,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(operationId))
            {
                return BadRequest("operationId is required.");
            }

            var operation = await _queryStore.FindByOperationIdAsync(
                operationId, cancellationToken);

            if (operation is null)
            {
                return NotFound();
            }

            return Ok(new RegisterDocumentResponse(
                OperationId: operation.Value.OperationId,
                Status: operation.Value.Status));
        }
    }
}
