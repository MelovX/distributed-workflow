using Microsoft.EntityFrameworkCore;

namespace Interview.Playground.Api.Data.Entities
{
    [Index(nameof(IdempotencyKey), IsUnique = true)]
    public sealed class RegistrationOperation
    {
        public string Id { get; set; } = default!;

        public string IdempotencyKey { get; set; } = default!;

        public string DocumentId { get; set; } = default!;

        public string Title { get; set; } = default!;

        public string Status { get; set; } = default!;

        public DateTimeOffset CreatedAt { get; set; }
    }
}
