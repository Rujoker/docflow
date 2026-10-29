namespace DocFlow.Persistence;

public sealed class ProcessedMessage
{
    public Guid MessageId { get; set; }
    public required string Consumer { get; set; }
    public DateTimeOffset ProcessedAt { get; set; }

    public static ProcessedMessage Create(Guid messageId, string consumer) => new()
    {
        MessageId = messageId,
        Consumer = consumer,
        ProcessedAt = DateTimeOffset.UtcNow
    };
}
