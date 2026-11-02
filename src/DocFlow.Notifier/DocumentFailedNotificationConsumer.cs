using DocFlow.Contracts;
using MassTransit;

namespace DocFlow.Notifier;

public sealed class DocumentFailedNotificationConsumer(ILogger<DocumentFailedNotificationConsumer> logger)
    : IConsumer<DocumentFailed>
{
    public Task Consume(ConsumeContext<DocumentFailed> context)
    {
        var analyzed = context.Message;
        logger.LogInformation(
            "Email to user: could not be analyzed: {Reason}",
            analyzed.Reason);
        return Task.CompletedTask;
    }
}
