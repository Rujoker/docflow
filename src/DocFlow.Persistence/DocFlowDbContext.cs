using MassTransit;
using Microsoft.EntityFrameworkCore;

namespace DocFlow.Persistence;

public sealed class DocFlowDbContext(DbContextOptions<DocFlowDbContext> options) : DbContext(options)
{
    public DbSet<Document> Documents => Set<Document>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Document>()
            .Property(document => document.Status)
            .HasConversion<string>();

        modelBuilder.AddInboxStateEntity();
        modelBuilder.AddOutboxMessageEntity();
        modelBuilder.AddOutboxStateEntity();
    }
}
