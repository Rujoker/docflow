namespace DocFlow.Contracts;

public sealed record AnalyzeDocument(
    Guid DocumentId,
    string StoragePath,
    string FileName,
    DateTimeOffset CreatedAt);