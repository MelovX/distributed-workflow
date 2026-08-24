using DistributedWorkflow.Worker.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace DistributedWorkflow.Worker.Data
{
    public sealed class RegistrationDbContext : DbContext
    {
        public RegistrationDbContext(DbContextOptions<RegistrationDbContext> options)
            : base(options)
        {
        }

        public DbSet<RegistrationOperation> RegistrationOperations => Set<RegistrationOperation>();
    }
}
