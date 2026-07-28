using Interview.Playground.Worker.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Interview.Playground.Worker.Data
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
