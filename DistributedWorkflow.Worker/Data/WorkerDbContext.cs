using DistributedWorkflow.Worker.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace DistributedWorkflow.Worker.Data
{
    public sealed class WorkerDbContext(
        DbContextOptions<WorkerDbContext> options)
        : DbContext(options)
    {
        public DbSet<InboxMessage> InboxMessages =>
            Set<InboxMessage>();

        public DbSet<KafkaOutboxMessage> KafkaOutboxMessages =>
            Set<KafkaOutboxMessage>();

        protected override void OnModelCreating(
            ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<InboxMessage>(entity =>
            {
                entity.ToTable("WorkerInboxMessages");

                entity.HasKey(message => message.MessageId);
            });

            modelBuilder.Entity<KafkaOutboxMessage>(entity =>
            {
                entity.ToTable("WorkerKafkaOutboxMessages");

                entity.HasKey(message => message.Id);

                entity.HasIndex(message => new
                {
                    message.CreatedAt,
                    message.Id
                })
                    .HasDatabaseName(
                        "IX_WorkerKafkaOutbox_Unpublished_CreatedAt_Id")
                    .HasFilter("\"PublishedAt\" IS NULL")
                    .IsCreatedConcurrently();
            });
        }
    }
}