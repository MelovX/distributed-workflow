namespace Interview.Playground.Api.Models
{
    public sealed record RegisterDocumentCommand(string OperationId, string DocumentId,
        string Title, DateTimeOffset CreatedAt);
}
