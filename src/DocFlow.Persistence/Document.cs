namespace DocFlow.Persistence;

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
