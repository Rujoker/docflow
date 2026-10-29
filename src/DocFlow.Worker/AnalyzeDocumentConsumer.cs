using System.Data.Common;
using System.Text;
using System.Text.Json;
using DocFlow.Contracts;
using DocFlow.Messaging;
using DocFlow.Persistence;
using DocFlow.Worker.Llm;
using Microsoft.EntityFrameworkCore;
using Polly;
using Polly.Retry;
using Polly.Timeout;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DocFlow.Worker;

public sealed class AnalyzeDocumentConsumer(
    IConnection connection,
    IServiceScopeFactory scopes,
    ILlmClient llm,
    RabbitPublisher publisher,
    IConfiguration configuration,
    ILogger<AnalyzeDocumentConsumer> logger) : BackgroundService
{
    private const string ConsumerName = "worker";
    private const int MaxRetries = 3;
    private const string CrashBeforeCommitKey = "Chaos:CrashBeforeCommit";

    private static readonly ResiliencePipeline Resilience = new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder()
                .Handle<LlmTransientException>()
                .Handle<TimeoutRejectedException>(),
            MaxRetryAttempts = 2,
            Delay = TimeSpan.FromMilliseconds(300),
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true
        })
        .AddTimeout(TimeSpan.FromSeconds(30))
        .Build();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        await Topology.DeclareAsync(channel, stoppingToken);
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, delivery) => HandleAsync(channel, delivery, stoppingToken);

        await channel.BasicConsumeAsync(Topology.AnalyzeQueue, autoAck: false, consumer, stoppingToken);
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task HandleAsync(IChannel channel, BasicDeliverEventArgs delivery, CancellationToken ct)
    {
        var attempt = GetDeathCount(delivery.BasicProperties) + 1;
        AnalyzeDocument? command = null;

        try
        {
            var messageId = ParseMessageId(delivery.BasicProperties);
            command = JsonSerializer.Deserialize<AnalyzeDocument>(delivery.Body.Span)
                ?? throw new InvalidDocumentException("Empty message body");

            logger.LogInformation(
                "Start {DocumentId}, message {MessageId}, attempt {Attempt}, redelivered={Redelivered}",
                command.DocumentId, messageId, attempt, delivery.Redelivered);

            await AnalyzeAsync(messageId, command, attempt, ct);
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            logger.LogInformation(
                "Shutdown during processing of {MessageId}, leaving it unacked",
                delivery.BasicProperties.MessageId);
        }
        catch (Exception ex) when (IsTransient(ex) && attempt <= MaxRetries)
        {
            logger.LogWarning(
                "Transient failure on attempt {Attempt}, retry in {RetryDelay}: {Error}",
                attempt, Topology.RetryDelay, ex.Message);
            await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: false, ct);
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Giving up on message {MessageId} after {Attempt} attempts",
                delivery.BasicProperties.MessageId, attempt);

            if (command is not null)
                await MarkFailedAsync(command.DocumentId, ex, ct);

            await publisher.PublishToDeadLetterAsync(delivery.Body, delivery.BasicProperties, ex, attempt, ct);
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, ct);
        }
    }

    private async Task AnalyzeAsync(Guid messageId, AnalyzeDocument command, long attempt, CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DocFlowDbContext>();

        if (await db.ProcessedMessages.IsProcessedAsync(messageId, ConsumerName, ct))
        {
            logger.LogInformation("Duplicate message {MessageId}, skipping", messageId);
            return;
        }

        var document = await db.Documents.FindAsync([command.DocumentId], ct);
        if (document is not { Status: DocumentStatus.Pending })
        {
            logger.LogInformation(
                "Document {DocumentId} is {Status}, skipping",
                command.DocumentId, document?.Status.ToString() ?? "missing");
            return;
        }

        var text = await File.ReadAllTextAsync(command.StoragePath, ct);
        var result = await Resilience.ExecuteAsync(async token => await llm.AnalyzeAsync(text, token), ct);

        if (configuration.GetValue<bool>(CrashBeforeCommitKey))
            Environment.FailFast("Chaos: crashed after the LLM call, before commit");

        var analyzedAt = DateTimeOffset.UtcNow;
        document.Status = DocumentStatus.Analyzed;
        document.AnalyzedAt = analyzedAt;
        document.IssuesFound = result.IssuesFound;
        document.Summary = result.Summary;

        db.ProcessedMessages.Add(ProcessedMessage.Create(messageId, ConsumerName));
        db.OutboxMessages.Add(OutboxMessage.Create(
            Topology.EventsExchange,
            Topology.DocumentAnalyzedRoutingKey,
            new DocumentAnalyzed(document.Id, result.IssuesFound, result.Summary, analyzedAt)));

        try
        {
            await db.SaveChangesAsync(ct);
            logger.LogInformation(
                "Analyzed {DocumentId}: {Summary} (attempt {Attempt})",
                command.DocumentId, result.Summary, attempt);
        }
        catch (DbUpdateException ex) when (ex.IsUniqueViolation())
        {
            logger.LogInformation("Duplicate message {MessageId} detected at commit, result discarded", messageId);
        }
    }

    private async Task MarkFailedAsync(Guid documentId, Exception error, CancellationToken ct)
    {
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<DocFlowDbContext>();

            var document = await db.Documents.FindAsync([documentId], ct);
            if (document is not { Status: DocumentStatus.Pending })
                return;

            document.Status = DocumentStatus.Failed;
            db.OutboxMessages.Add(OutboxMessage.Create(
                Topology.EventsExchange,
                Topology.DocumentFailedRoutingKey,
                new DocumentFailed(documentId, error.Message, DateTimeOffset.UtcNow)));

            await db.SaveChangesAsync(ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning("Could not mark document {DocumentId} as failed: {Error}", documentId, ex.Message);
        }
    }

    private static Guid ParseMessageId(IReadOnlyBasicProperties properties) =>
        Guid.TryParse(properties.MessageId, out var messageId)
            ? messageId
            : throw new InvalidDocumentException("Message has no valid MessageId");

    private static bool IsTransient(Exception exception) =>
        exception is LlmTransientException or TimeoutRejectedException or DbException;

    private static long GetDeathCount(IReadOnlyBasicProperties properties)
    {
        if (properties.Headers is null
            || !properties.Headers.TryGetValue("x-death", out var header)
            || header is not IList<object> deaths)
        {
            return 0;
        }

        foreach (var entry in deaths)
        {
            if (entry is not IDictionary<string, object> death)
                continue;

            var queue = Encoding.UTF8.GetString((byte[])death["queue"]);
            if (queue == Topology.AnalyzeQueue)
                return (long)death["count"];
        }

        return 0;
    }
}
