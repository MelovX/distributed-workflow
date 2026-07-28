using Interview.Playground.Api.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Interview.Playground.Api.Data
{
    public sealed class RegistrationDbContext(DbContextOptions<RegistrationDbContext> options) : DbContext(options)
    {
        public DbSet<RegistrationOperation> RegistrationOperations => Set<RegistrationOperation>();

        public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    }
}
