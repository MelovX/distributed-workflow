using DistributedWorkflow.Api.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace DistributedWorkflow.Api.Data
{
    public sealed class RegistrationDbContext(DbContextOptions<RegistrationDbContext> options) : DbContext(options)
    {
        public DbSet<RegistrationOperation> RegistrationOperations => Set<RegistrationOperation>();

        public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<OutboxMessage>()
                .HasIndex(message => new
                {
                    message.CreatedAt,
                    message.Id
                })
                .HasDatabaseName(
                    "IX_OutboxMessages_Unpublished_CreatedAt_Id")
                .HasFilter("\"PublishedAt\" IS NULL")
                .IsCreatedConcurrently();
        }
    }
}
