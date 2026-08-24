using Microsoft.AspNetCore.Mvc;

namespace DistributedWorkflow.Api.Controllers
{
    using global::DistributedWorkflow.Api.Models;
    using Microsoft.AspNetCore.Mvc;
    using StackExchange.Redis;


    [ApiController]
    [Route("locks")]
    public sealed class LocksController : ControllerBase
    {
        private static readonly TimeSpan LockTtl = TimeSpan.FromSeconds(60);

        private readonly IConnectionMultiplexer _redis;

        public LocksController(IConnectionMultiplexer redis)
        {
            _redis = redis;
        }

        [HttpPost("{name}/acquire")]
        public async Task<ActionResult<LockResponse>> AcquireLock(string name)
        {
            var db = _redis.GetDatabase();

            var lockId = Guid.NewGuid().ToString("N");
            var key = GetLockKey(name);

            var acquired = await db.StringSetAsync(
                key,
                lockId,
                expiry: LockTtl,
                when: When.NotExists);

            if (!acquired)
            {
                var ttl = await db.KeyTimeToLiveAsync(key);

                return Conflict(new LockResponse(
                    Name: name,
                    Acquired: false,
                    LockId: null,
                    TtlSeconds: ttl is null ? 0 : (int) ttl.Value.TotalSeconds));
            }

            return Ok(new LockResponse(
                Name: name,
                Acquired: true,
                LockId: lockId,
                TtlSeconds: (int) LockTtl.TotalSeconds));
        }

        [HttpGet("{name}")]
        public async Task<ActionResult<object>> GetLock(string name)
        {
            var db = _redis.GetDatabase();

            var key = GetLockKey(name);
            var lockId = await db.StringGetAsync(key);
            var ttl = await db.KeyTimeToLiveAsync(key);

            if (lockId.IsNullOrEmpty)
            {
                return NotFound(new
                {
                    Name = name,
                    Locked = false
                });
            }

            return Ok(new
            {
                Name = name,
                Locked = true,
                LockId = lockId.ToString(),
                TtlSeconds = ttl is null ? 0 : (int) ttl.Value.TotalSeconds
            });
        }

        [HttpDelete("{name}")]
        public async Task<IActionResult> ReleaseLock(
            string name,
            [FromHeader(Name = "Lock-Id")] string? lockId)
        {
            if (string.IsNullOrWhiteSpace(lockId))
            {
                return BadRequest("Lock-Id header is required.");
            }

            var db = _redis.GetDatabase();
            var key = GetLockKey(name);

            var script = """
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
            """;

            var result = (int) await db.ScriptEvaluateAsync(
                script,
                keys: new RedisKey[] { key },
                values: new RedisValue[] { lockId });

            return result == 1 ? NoContent() : Conflict("Lock is owned by another process or already expired.");
        }

        private static string GetLockKey(string name)
        {
            return $"lock:{name}";
        }
    }
}
