using Microsoft.EntityFrameworkCore;

namespace DistributedWorkflow.OutboxPublisher.Data
{
    public sealed class OutboxDbContext(DbContextOptions<OutboxDbContext> options)
        : DbContext(options)
    {
        public DbSet<OutboxMessage> OutboxMessages =>
            Set<OutboxMessage>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<OutboxMessage>(entity =>
            {
                entity.ToTable("OutboxMessages");
                entity.HasKey(message => message.Id);
            });
        }
    }
}
