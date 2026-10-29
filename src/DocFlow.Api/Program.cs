using DocFlow.Contracts;
using DocFlow.Messaging;
using DocFlow.Persistence;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddRabbit();
builder.Services.AddDbContext<DocFlowDbContext>(o => o
    .UseNpgsql(builder.Configuration.GetConnectionString("DocFlow"))
    .UseSnakeCaseNamingConvention());
builder.Services.AddHostedService<OutboxRelay>();

var app = builder.Build();

await using (var channel = await app.Services.GetRequiredService<IConnection>().CreateChannelAsync())
    await Topology.DeclareAsync(channel);

var storageRoot = StoragePaths.Resolve(app.Configuration["Storage:Root"] ?? "storage");
Directory.CreateDirectory(storageRoot);
app.Logger.LogInformation("Storage root: {StorageRoot}", storageRoot);

app.MapPost("/documents", async (IFormFile file, DocFlowDbContext db, CancellationToken ct) =>
{
    var documentId = Guid.NewGuid();
    var path = Path.Combine(storageRoot, $"{documentId}{Path.GetExtension(file.FileName)}");

    await using (var fs = File.Create(path))
    {
        await file.CopyToAsync(fs, ct);
    }

    db.Documents.Add(new Document
    {
        Id = documentId, FileName = file.FileName, StoragePath = path,
        Status = DocumentStatus.Pending, CreatedAt = DateTimeOffset.UtcNow
    });
    db.OutboxMessages.Add(OutboxMessage.Create(Topology.CommandsExchange, Topology.AnalyzeRoutingKey,
        new AnalyzeDocument(documentId, path, file.FileName, DateTimeOffset.UtcNow)));

    await db.SaveChangesAsync(ct);
    return Results.Accepted($"/documents/{documentId}", new { documentId });
}).DisableAntiforgery();

app.MapGet("/documents/{id:guid}", async (Guid id, DocFlowDbContext db) =>
    await db.Documents.FindAsync(id) is { } doc ? Results.Ok(doc) : Results.NotFound());

app.Run();