namespace Interview.Playground.Api.Models
{
    public sealed record BffSession(string SessionId, string UserId, string UserName,
        DateTimeOffset CreatedAt, DateTimeOffset LastActiveAt);
}
