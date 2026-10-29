using System.Data.Common;
using System.Text;
using System.Text.Json;
using DocFlow.Contracts;
using DocFlow.Messaging;
using DocFlow.Persistence;
using Microsoft.EntityFrameworkCore;
using Npgsql;
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
    IConfiguration config,
    ILogger<AnalyzeDocumentConsumer> logger) : BackgroundService
{
    private const string ConsumerName = "worker";

    private static readonly ResiliencePipeline Resilience = new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder().Handle<LlmTransientException>().Handle<TimeoutRejectedException>(),
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
        consumer.ReceivedAsync += (_, ea) => HandleAsync(channel, ea, stoppingToken);

        await channel.BasicConsumeAsync(Topology.AnalyzeQueue, autoAck: false, consumer, stoppingToken);
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task HandleAsync(IChannel channel, BasicDeliverEventArgs ea, CancellationToken ct)
    {
        var attempt = DeathCount(ea.BasicProperties) + 1;
        AnalyzeDocument? command = null;

        try
        {
            if (!Guid.TryParse(ea.BasicProperties.MessageId, out var messageId))
                throw new InvalidDocumentException("Message has no valid MessageId");

            command = JsonSerializer.Deserialize<AnalyzeDocument>(ea.Body.Span)
                ?? throw new InvalidDocumentException("Empty message body");
            logger.LogInformation("Start {DocumentId}, message {MessageId}, attempt {Attempt}, redelivered={Redelivered}",
                command.DocumentId, messageId, attempt, ea.Redelivered);

            await using var scope = scopes.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<DocFlowDbContext>();

            if (await db.ProcessedMessages.AnyAsync(p => p.MessageId == messageId && p.Consumer == ConsumerName, ct))
            {
                logger.LogInformation("Duplicate message {MessageId}, skipping", messageId);
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ct);
                return;
            }

            var document = await db.Documents.FindAsync([command.DocumentId], ct);
            if (document is null || document.Status != DocumentStatus.Pending)
            {
                logger.LogInformation("Document {DocumentId} is {Status}, skipping",
                    command.DocumentId, document?.Status.ToString() ?? "missing");
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ct);
                return;
            }

            var text = await File.ReadAllTextAsync(command.StoragePath, ct);
            var result = await Resilience.ExecuteAsync(async token => await llm.AnalyzeAsync(text, token), ct);

            if (config.GetValue<bool>("Chaos:CrashBeforeCommit"))
                Environment.FailFast("Chaos: crashed after the LLM call, before commit");

            document.Status = DocumentStatus.Analyzed;
            document.AnalyzedAt = DateTimeOffset.UtcNow;
            document.IssuesFound = result.IssuesFound;
            document.Summary = result.Summary;
            db.ProcessedMessages.Add(new ProcessedMessage
            {
                MessageId = messageId, Consumer = ConsumerName, ProcessedAt = DateTimeOffset.UtcNow
            });
            db.OutboxMessages.Add(OutboxMessage.Create(Topology.EventsTopic, Topology.DocumentAnalyzed,
                new DocumentAnalyzed(document.Id, result.IssuesFound, result.Summary, document.AnalyzedAt.Value)));

            try
            {
                await db.SaveChangesAsync(ct);
                logger.LogInformation("Analyzed {DocumentId}: {Summary} (attempt {Attempt})",
                    command.DocumentId, result.Summary, attempt);
            }
            catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation })
            {
                logger.LogInformation("Duplicate message {MessageId} detected at commit, result discarded", messageId);
            }

            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            logger.LogInformation("Shutdown during processing of {MessageId}, leaving it unacked", ea.BasicProperties.MessageId);
        }
        catch (Exception ex) when (IsTransient(ex) && attempt <= Topology.MaxRetries)
        {
            logger.LogWarning("Transient failure on attempt {Attempt}, retry in 10s: {Error}", attempt, ex.Message);
            await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Giving up on message {MessageId} after {Attempt} attempts",
                ea.BasicProperties.MessageId, attempt);
            if (command is not null)
                await MarkFailedAsync(command.DocumentId, ex, ct);
            await publisher.PublishToDeadLetterAsync(ea.Body, ea.BasicProperties, ex, attempt, ct);
            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ct);
        }
    }

    private async Task MarkFailedAsync(Guid documentId, Exception error, CancellationToken ct)
    {
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<DocFlowDbContext>();

            var document = await db.Documents.FindAsync([documentId], ct);
            if (document is null || document.Status != DocumentStatus.Pending)
                return;

            document.Status = DocumentStatus.Failed;
            db.OutboxMessages.Add(OutboxMessage.Create(Topology.EventsTopic, Topology.DocumentFailed,
                new DocumentFailed(documentId, error.Message, DateTimeOffset.UtcNow)));
            await db.SaveChangesAsync(ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning("Could not mark document {DocumentId} as failed: {Error}", documentId, ex.Message);
        }
    }

    private static bool IsTransient(Exception ex) =>
        ex is LlmTransientException or TimeoutRejectedException or DbException;

    private static long DeathCount(IReadOnlyBasicProperties props)
    {
        if (props.Headers is null || !props.Headers.TryGetValue("x-death", out var raw) || raw is not IList<object> deaths)
            return 0;

        foreach (var item in deaths)
        {
            if (item is not IDictionary<string, object> death) continue;
            var queue = Encoding.UTF8.GetString((byte[])death["queue"]);
            if (queue == Topology.AnalyzeQueue)
                return (long)death["count"];
        }
        return 0;
    }
}
