namespace Interview.Playground.Worker.Models
{
    public sealed record RegisterDocumentCommand(string OperationId, string DocumentId,
        string Title, DateTimeOffset CreatedAt);
}
