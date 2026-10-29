using Microsoft.EntityFrameworkCore;

namespace DocFlow.Persistence;

public static class ProcessedMessageQueries
{
    public static Task<bool> IsProcessedAsync(
        this IQueryable<ProcessedMessage> processedMessages,
        Guid messageId,
        string consumer,
        CancellationToken ct = default) =>
        processedMessages.AnyAsync(message => message.MessageId == messageId && message.Consumer == consumer, ct);
}
