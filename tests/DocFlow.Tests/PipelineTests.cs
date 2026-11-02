using System.Net.Http.Json;
using System.Text.Json;
using DocFlow.Contracts;
using DocFlow.Messaging;
using DocFlow.Persistence;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace DocFlow.Tests;

[Collection(nameof(DocFlowCollection))]
public sealed class PipelineTests(DocFlowFixture fixture)
{
    private readonly HttpClient _api = fixture.Api.CreateClient();

    [Fact]
    public async Task Uploaded_document_is_analyzed_once()
    {
        var text = $"Document {Guid.NewGuid()}: we share data with third parties.";

        var documentId = await UploadAsync(text);
        var document = await Eventually.GetAsync(() => GetDocumentAsync(documentId), d => d.Status == "Analyzed");

        Assert.Equal("Analyzed", document.Status);
        Assert.Equal(1, document.IssuesFound);
        Assert.Equal(1, fixture.Llm.CallsFor(text));
    }

    [Fact]
    public async Task Duplicate_message_is_processed_once()
    {
        var text = $"Document {Guid.NewGuid()}: we share data with third parties.";
        var markerText = $"Marker {Guid.NewGuid()}";
        var command = await SeedPendingDocumentAsync(text);
        var marker = await SeedPendingDocumentAsync(markerText);
        var messageId = Guid.NewGuid();

        await using var connection = await ConnectAsync();
        await using var publisher = new RabbitPublisher(connection);
        await PublishCommandAsync(publisher, command, messageId);
        await PublishCommandAsync(publisher, command, messageId);
        await PublishCommandAsync(publisher, marker, Guid.NewGuid());

        var markerDocument = await Eventually.GetAsync(
            () => GetDocumentAsync(marker.DocumentId), d => d.Status == "Analyzed");
        var document = await GetDocumentAsync(command.DocumentId);

        Assert.Equal("Analyzed", markerDocument.Status);
        Assert.Equal("Analyzed", document.Status);
        Assert.Equal(1, fixture.Llm.CallsFor(text));
    }

    [Fact]
    public async Task Poison_document_fails_without_retries()
    {
        var text = $"Document {Guid.NewGuid()}: #poison";

        var documentId = await UploadAsync(text);
        var document = await Eventually.GetAsync(() => GetDocumentAsync(documentId), d => d.Status == "Failed");

        Assert.Equal("Failed", document.Status);
        Assert.Equal(1, fixture.Llm.CallsFor(text));
    }

    [Fact]
    public async Task Analyzed_document_publishes_event()
    {
        var text = $"Document {Guid.NewGuid()}: we share data with third parties.";

        await using var connection = await ConnectAsync();
        await using var channel = await connection.CreateChannelAsync();
        var queue = await channel.QueueDeclareAsync(queue: string.Empty, durable: false, exclusive: true, autoDelete: true);
        await channel.QueueBindAsync(queue.QueueName, Topology.EventsExchange, Topology.DocumentAnalyzedRoutingKey);

        var documentId = await UploadAsync(text);
        var analyzed = await Eventually.GetAsync(
            () => ReceiveAnalyzedAsync(channel, queue.QueueName, documentId), e => e is not null);

        Assert.NotNull(analyzed);
        Assert.Equal(1, analyzed.IssuesFound);
    }

    private async Task<Guid> UploadAsync(string text)
    {
        using var content = new MultipartFormDataContent { { new StringContent(text), "file", "document.txt" } };
        var response = await _api.PostAsync("/documents", content);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<UploadResponse>())!.DocumentId;
    }

    private async Task<DocumentResponse> GetDocumentAsync(Guid id) =>
        (await _api.GetFromJsonAsync<DocumentResponse>($"/documents/{id}"))!;

    private async Task<AnalyzeDocument> SeedPendingDocumentAsync(string text)
    {
        var documentId = Guid.NewGuid();
        var storageRoot = Environment.GetEnvironmentVariable("Storage__Root")!;
        Directory.CreateDirectory(storageRoot);
        var storagePath = Path.Combine(storageRoot, $"{documentId}.txt");
        await File.WriteAllTextAsync(storagePath, text);

        var createdAt = DateTimeOffset.UtcNow;
        await using var scope = fixture.Api.Services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DocFlowDbContext>();
        db.Documents.Add(new Document
        {
            Id = documentId,
            FileName = "document.txt",
            StoragePath = storagePath,
            Status = DocumentStatus.Pending,
            CreatedAt = createdAt
        });
        await db.SaveChangesAsync();

        return new AnalyzeDocument(documentId, storagePath, "document.txt", createdAt);
    }

    private Task<IConnection> ConnectAsync() =>
        new ConnectionFactory { Uri = new Uri(fixture.RabbitMqConnectionString) }.CreateConnectionAsync();

    private static Task PublishCommandAsync(RabbitPublisher publisher, AnalyzeDocument command, Guid messageId) =>
        publisher.PublishAsync(
            Topology.CommandsExchange,
            Topology.AnalyzeRoutingKey,
            nameof(AnalyzeDocument),
            JsonSerializer.SerializeToUtf8Bytes(command),
            messageId);

    private static async Task<DocumentAnalyzed?> ReceiveAnalyzedAsync(IChannel channel, string queue, Guid documentId)
    {
        while (await channel.BasicGetAsync(queue, autoAck: true) is { } message)
        {
            var analyzed = JsonSerializer.Deserialize<DocumentAnalyzed>(message.Body.Span);
            if (analyzed?.DocumentId == documentId)
                return analyzed;
        }

        return null;
    }

    private sealed record UploadResponse(Guid DocumentId);

    private sealed record DocumentResponse(Guid Id, string Status, int? IssuesFound);
}
