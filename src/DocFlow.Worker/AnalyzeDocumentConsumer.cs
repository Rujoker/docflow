using DocFlow.Contracts;
using DocFlow.Persistence;
using DocFlow.Worker.Llm;
using MassTransit;

namespace DocFlow.Worker;

public sealed class AnalyzeDocumentConsumer(
    DocFlowDbContext db,
    ILlmClient llm,
    ILogger<AnalyzeDocumentConsumer> logger) : IConsumer<AnalyzeDocument>
{
    public async Task Consume(ConsumeContext<AnalyzeDocument> context)
    {
        var command = context.Message;
        var ct = context.CancellationToken;

        var document = await db.Documents.FindAsync([command.DocumentId], ct);
        if (document is not { Status: DocumentStatus.Pending })
        {
            logger.LogInformation(
                "Document {DocumentId} is {Status}, skipping",
                command.DocumentId, document?.Status.ToString() ?? "missing");
            return;
        }

        var text = await File.ReadAllTextAsync(command.StoragePath, ct);
        var result = await llm.AnalyzeAsync(text, ct);

        var analyzedAt = DateTimeOffset.UtcNow;
        document.Status = DocumentStatus.Analyzed;
        document.AnalyzedAt = analyzedAt;
        document.IssuesFound = result.IssuesFound;
        document.Summary = result.Summary;

        await context.Publish(new DocumentAnalyzed(document.Id, result.IssuesFound, result.Summary, analyzedAt), ct);
        await db.SaveChangesAsync(ct);

        logger.LogInformation("Analyzed {DocumentId}: {Summary}", command.DocumentId, result.Summary);
    }
}
