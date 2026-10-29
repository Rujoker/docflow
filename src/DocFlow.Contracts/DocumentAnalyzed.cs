namespace DocFlow.Contracts;

public sealed record DocumentAnalyzed(
    Guid DocumentId,
    int IssuesFound,
    string Summary,
    DateTimeOffset AnalyzedAt);
