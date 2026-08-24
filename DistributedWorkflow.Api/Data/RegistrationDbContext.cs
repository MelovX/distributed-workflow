using DistributedWorkflow.Api.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace DistributedWorkflow.Api.Data
{
    public sealed class RegistrationDbContext(DbContextOptions<RegistrationDbContext> options) : DbContext(options)
    {
        public DbSet<RegistrationOperation> RegistrationOperations => Set<RegistrationOperation>();

        public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    }
}
