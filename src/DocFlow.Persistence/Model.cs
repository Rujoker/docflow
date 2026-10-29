using System.Text.Json;
using Microsoft.EntityFrameworkCore;

namespace DocFlow.Persistence;

public enum DocumentStatus { Pending, Analyzed, Failed }

public sealed class Document
{
    public Guid Id { get; set; }
    public required string FileName { get; set; }
    public required string StoragePath { get; set; }
    public DocumentStatus Status { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? AnalyzedAt { get; set; }
    public int? IssuesFound { get; set; }
    public string? Summary { get; set; }
}

public sealed class OutboxMessage
{
    public Guid Id { get; set; }
    public required string Exchange { get; set; }
    public required string RoutingKey { get; set; }
    public required string Type { get; set; }
    public required string Payload { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? ProcessedAt { get; set; }

    public static OutboxMessage Create<T>(string exchange, string routingKey, T message) => new()
    {
        Id = Guid.NewGuid(),
        Exchange = exchange,
        RoutingKey = routingKey,
        Type = typeof(T).Name,
        Payload = JsonSerializer.Serialize(message),
        CreatedAt = DateTimeOffset.UtcNow
    };
}

public sealed class ProcessedMessage
{
    public Guid MessageId { get; set; }
    public required string Consumer { get; set; }
    public DateTimeOffset ProcessedAt { get; set; }
}

public sealed class DocFlowDbContext(DbContextOptions<DocFlowDbContext> options) : DbContext(options)
{
    public DbSet<Document> Documents => Set<Document>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<ProcessedMessage> ProcessedMessages => Set<ProcessedMessage>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Document>().Property(d => d.Status).HasConversion<string>();
        b.Entity<OutboxMessage>().Property(m => m.Payload).HasColumnType("jsonb");
        b.Entity<OutboxMessage>().HasIndex(m => m.CreatedAt).HasFilter("processed_at IS NULL");
        b.Entity<ProcessedMessage>().HasKey(p => new { p.MessageId, p.Consumer });
    }
}