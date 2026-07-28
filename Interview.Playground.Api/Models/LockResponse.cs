namespace Interview.Playground.Api.Models
{
    public sealed record LockResponse(
        string Name,
        bool Acquired,
        string? LockId,
        int TtlSeconds);
}
