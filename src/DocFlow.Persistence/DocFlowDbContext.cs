using Microsoft.EntityFrameworkCore;

namespace DocFlow.Persistence;

public sealed class DocFlowDbContext(DbContextOptions<DocFlowDbContext> options) : DbContext(options)
{
    public DbSet<Document> Documents => Set<Document>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<ProcessedMessage> ProcessedMessages => Set<ProcessedMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Document>()
            .Property(document => document.Status)
            .HasConversion<string>();

        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            entity.Property(message => message.Payload).HasColumnType("jsonb");
            entity.HasIndex(message => message.CreatedAt).HasFilter("processed_at IS NULL");
        });

        modelBuilder.Entity<ProcessedMessage>()
            .HasKey(message => new { message.MessageId, message.Consumer });
    }
}
