namespace DocFlow.Contracts;

public sealed record DocumentFailed(
    Guid DocumentId,
    string Reason,
    DateTimeOffset FailedAt);
