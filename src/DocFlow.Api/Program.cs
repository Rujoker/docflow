using System.Text.Json.Serialization;
using DocFlow.Api;
using DocFlow.Contracts;
using DocFlow.Messaging;
using DocFlow.Persistence;
using MassTransit;
using MassTransit.EntityFrameworkCoreIntegration;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddPersistence(builder.Configuration);
builder.Services.AddMassTransit(bus =>
{
    bus.AddEntityFrameworkOutbox<DocFlowDbContext>(outbox =>
    {
        outbox.UsePostgres();
        outbox.UseBusOutbox();
    });

    bus.UsingRabbitMq((context, rabbit) =>
    {
        rabbit.UseDocFlowHost(builder.Configuration);
        rabbit.ConfigureEndpoints(context);
    });
});
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

var app = builder.Build();

var storageRoot = StoragePaths.Resolve(app.Configuration["Storage:Root"] ?? "storage");
Directory.CreateDirectory(storageRoot);
app.Logger.LogInformation("Storage root: {StorageRoot}", storageRoot);

app.MapPost("/documents", async (
    IFormFile file,
    DocFlowDbContext db,
    ISendEndpointProvider sendEndpoints,
    CancellationToken ct) =>
{
    var documentId = Guid.NewGuid();
    var storagePath = Path.Combine(storageRoot, $"{documentId}{Path.GetExtension(file.FileName)}");

    await using (var stream = File.Create(storagePath))
    {
        await file.CopyToAsync(stream, ct);
    }

    var createdAt = DateTimeOffset.UtcNow;
    db.Documents.Add(new Document
    {
        Id = documentId,
        FileName = file.FileName,
        StoragePath = storagePath,
        Status = DocumentStatus.Pending,
        CreatedAt = createdAt
    });
    var endpoint = await sendEndpoints.GetSendEndpoint(new Uri($"queue:{BusConfiguration.AnalyzeDocumentQueue}"));
    await endpoint.Send(new AnalyzeDocument(documentId, storagePath, file.FileName, createdAt), ct);

    await db.SaveChangesAsync(ct);
    return Results.Accepted($"/documents/{documentId}", new { documentId });
}).DisableAntiforgery();

app.MapGet("/documents/{id:guid}", async (Guid id, DocFlowDbContext db, CancellationToken ct) =>
    await db.Documents.FindAsync([id], ct) is { } document
        ? Results.Ok(document)
        : Results.NotFound());

app.Run();
