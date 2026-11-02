using DocFlow.Contracts;
using MassTransit;

namespace DocFlow.Notifier;

public sealed class DocumentAnalyzedNotificationConsumer(ILogger<DocumentAnalyzedNotificationConsumer> logger)
    : IConsumer<DocumentAnalyzed>
{
    public Task Consume(ConsumeContext<DocumentAnalyzed> context)
    {
        var analyzed = context.Message;
        logger.LogInformation(
            "Email to user: document {DocumentId} analyzed, {IssuesFound} potential issues. {Summary}",
            analyzed.DocumentId, analyzed.IssuesFound, analyzed.Summary);
        return Task.CompletedTask;
    }
}
