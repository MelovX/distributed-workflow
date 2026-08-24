using DistributedWorkflow.Api.Models;
using Microsoft.AspNetCore.Mvc;
using StackExchange.Redis;
using System.Text.Json;

namespace DistributedWorkflow.Api.Controllers
{
    [ApiController]
    [Route("sessions")]
    public sealed class SessionsController : ControllerBase
    {
        private static readonly TimeSpan SessionTtl = TimeSpan.FromMinutes(5);
        private readonly IConnectionMultiplexer _redis;

        public SessionsController(IConnectionMultiplexer redis)
        {
            _redis = redis;
        }

        [HttpPost]
        public async Task<ActionResult<BffSession>> CreateSession()
        {
            var db = _redis.GetDatabase();

            var now = DateTimeOffset.UtcNow;

            var session = new BffSession(
                SessionId: Guid.NewGuid().ToString("N"),
                UserId: "user-123",
                UserName: "mikhail",
                CreatedAt: now,
                LastActiveAt: now);

            var key = GetSessionKey(session.SessionId);
            var json = JsonSerializer.Serialize(session);

            await db.StringSetAsync(
                key,
                json,
                expiry: SessionTtl);

            return CreatedAtAction(
                nameof(GetSession),
                new { sessionId = session.SessionId },
                session);
        }

        [HttpGet("{sessionId}")]
        public async Task<ActionResult<BffSession>> GetSession(string sessionId)
        {
            var db = _redis.GetDatabase();

            var key = GetSessionKey(sessionId);
            var json = await db.StringGetAsync(key);

            if (json.IsNullOrEmpty)
            {
                return NotFound();
            }

            var session = JsonSerializer.Deserialize<BffSession>(json!);

            return Ok(session);
        }

        [HttpDelete("{sessionId}")]
        public async Task<IActionResult> DeleteSession(string sessionId)
        {
            var db = _redis.GetDatabase();

            var key = GetSessionKey(sessionId);
            var deleted = await db.KeyDeleteAsync(key);

            return deleted ? NoContent() : NotFound();
        }

        [HttpPost("{sessionId}/touch")]
        public async Task<ActionResult<BffSession>> TouchSession(string sessionId)
        {
            var db = _redis.GetDatabase();

            var key = GetSessionKey(sessionId);
            var json = await db.StringGetAsync(key);

            if (json.IsNullOrEmpty)
            {
                return NotFound();
            }

            var session = JsonSerializer.Deserialize<BffSession>(json!);

            if (session is null)
            {
                return NotFound();
            }

            var updatedSession = session with
            {
                LastActiveAt = DateTimeOffset.UtcNow
            };

            var updatedJson = JsonSerializer.Serialize(updatedSession);

            await db.StringSetAsync(
                key,
                updatedJson,
                expiry: SessionTtl);

            return Ok(updatedSession);
        }

        [HttpGet("{sessionId}/ttl")]
        public async Task<ActionResult<object>> GetSessionTtl(string sessionId)
        {
            var db = _redis.GetDatabase();

            var key = GetSessionKey(sessionId);
            var ttl = await db.KeyTimeToLiveAsync(key);

            if (ttl is null)
            {
                return NotFound();
            }

            return Ok(new
            {
                SessionId = sessionId,
                TtlSeconds = (int) ttl.Value.TotalSeconds
            });
        }

        private static string GetSessionKey(string sessionId)
        {
            return $"bff:session:{sessionId}";
        }
    }
}
