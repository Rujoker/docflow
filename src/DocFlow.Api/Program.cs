using System.Text.Json.Serialization;
using DocFlow.Api;
using DocFlow.Contracts;
using DocFlow.Messaging;
using DocFlow.Persistence;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRabbitMq(builder.Configuration);
builder.Services.AddPersistence(builder.Configuration);
builder.Services.AddHostedService<OutboxRelay>();
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

var app = builder.Build();

await using (var channel = await app.Services.GetRequiredService<IConnection>().CreateChannelAsync())
{
    await Topology.DeclareAsync(channel);
}

var storageRoot = StoragePaths.Resolve(app.Configuration["Storage:Root"] ?? "storage");
Directory.CreateDirectory(storageRoot);
app.Logger.LogInformation("Storage root: {StorageRoot}", storageRoot);

app.MapPost("/documents", async (IFormFile file, DocFlowDbContext db, CancellationToken ct) =>
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
    db.OutboxMessages.Add(OutboxMessage.Create(
        Topology.CommandsExchange,
        Topology.AnalyzeRoutingKey,
        new AnalyzeDocument(documentId, storagePath, file.FileName, createdAt)));

    await db.SaveChangesAsync(ct);
    return Results.Accepted($"/documents/{documentId}", new { documentId });
}).DisableAntiforgery();

app.MapGet("/documents/{id:guid}", async (Guid id, DocFlowDbContext db, CancellationToken ct) =>
    await db.Documents.FindAsync([id], ct) is { } document
        ? Results.Ok(document)
        : Results.NotFound());

app.Run();
