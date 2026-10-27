using DocFlow.Contracts;
using DocFlow.Messaging;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddRabbit();
var app = builder.Build();

await using (var channel = await app.Services.GetRequiredService<IConnection>().CreateChannelAsync())
    await Topology.DeclareAsync(channel);

var storageRoot = StoragePaths.Resolve(app.Configuration["Storage:Root"] ?? "storage");
Directory.CreateDirectory(storageRoot);
app.Logger.LogInformation("Storage root: {StorageRoot}", storageRoot);

app.MapPost("/documents", async (IFormFile file, RabbitPublisher publisher, CancellationToken ct) =>
{
    var documentId = Guid.NewGuid();
    var path = Path.Combine(storageRoot, $"{documentId}{Path.GetExtension(file.FileName)}");

    await using (var fs = File.Create(path))
        await file.CopyToAsync(fs, ct);

    var command = new AnalyzeDocument(documentId, path, file.FileName, DateTimeOffset.UtcNow);
    await publisher.PublishAsync(Topology.CommandsExchange, Topology.AnalyzeRoutingKey, command, Guid.NewGuid(), ct);

    return Results.Accepted($"/documents/{documentId}", new { documentId });
}).DisableAntiforgery();

app.Run();