using DocFlow.Contracts;
using DocFlow.Persistence;
using MassTransit;

namespace DocFlow.Worker;

public sealed class AnalyzeDocumentFaultConsumer(DocFlowDbContext db) : IConsumer<Fault<AnalyzeDocument>>
{
    public async Task Consume(ConsumeContext<Fault<AnalyzeDocument>> context)
    {
        var documentId = context.Message.Message.DocumentId;
        var ct = context.CancellationToken;

        var document = await db.Documents.FindAsync([documentId], ct);
        if (document is not { Status: DocumentStatus.Pending })
            return;

        var reason = context.Message.Exceptions.FirstOrDefault()?.Message ?? "Unknown error";
        document.Status = DocumentStatus.Failed;

        await context.Publish(new DocumentFailed(documentId, reason, DateTimeOffset.UtcNow), ct);
        await db.SaveChangesAsync(ct);
    }
}
