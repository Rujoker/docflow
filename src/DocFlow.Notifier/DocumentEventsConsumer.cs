using System.Data.Common;
using System.Text.Json;
using DocFlow.Contracts;
using DocFlow.Messaging;
using DocFlow.Persistence;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DocFlow.Notifier;

public sealed class DocumentEventsConsumer(
    IConnection connection,
    IServiceScopeFactory scopes,
    ILogger<DocumentEventsConsumer> logger) : BackgroundService
{
    private const string ConsumerName = "notifier";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await channel.ExchangeDeclareAsync(Topology.EventsTopic, ExchangeType.Topic, durable: true, cancellationToken: stoppingToken);
        await channel.QueueDeclareAsync(Topology.NotifierQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" }, cancellationToken: stoppingToken);
        await channel.QueueBindAsync(Topology.NotifierQueue, Topology.EventsTopic, Topology.DocumentEventsPattern,
            cancellationToken: stoppingToken);
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false, stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, ea) => HandleAsync(channel, ea, stoppingToken);

        await channel.BasicConsumeAsync(Topology.NotifierQueue, autoAck: false, consumer, stoppingToken);
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task HandleAsync(IChannel channel, BasicDeliverEventArgs ea, CancellationToken ct)
    {
        try
        {
            if (!Guid.TryParse(ea.BasicProperties.MessageId, out var messageId))
            {
                logger.LogError("Event without a valid MessageId dropped");
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ct);
                return;
            }

            await using var scope = scopes.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<DocFlowDbContext>();

            if (await db.ProcessedMessages.AnyAsync(p => p.MessageId == messageId && p.Consumer == ConsumerName, ct))
            {
                logger.LogInformation("Duplicate event {MessageId}, skipping", messageId);
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ct);
                return;
            }

            switch (ea.BasicProperties.Type)
            {
                case nameof(DocumentAnalyzed):
                    var analyzed = JsonSerializer.Deserialize<DocumentAnalyzed>(ea.Body.Span)!;
                    logger.LogInformation("Email to user: document {DocumentId} analyzed, {IssuesFound} potential issues. {Summary}",
                        analyzed.DocumentId, analyzed.IssuesFound, analyzed.Summary);
                    break;

                case nameof(DocumentFailed):
                    var failed = JsonSerializer.Deserialize<DocumentFailed>(ea.Body.Span)!;
                    logger.LogInformation("Email to user: document {DocumentId} could not be analyzed: {Reason}",
                        failed.DocumentId, failed.Reason);
                    break;

                default:
                    logger.LogWarning("Unknown event type {Type}, message {MessageId} ignored", ea.BasicProperties.Type, messageId);
                    break;
            }

            db.ProcessedMessages.Add(new ProcessedMessage
            {
                MessageId = messageId, Consumer = ConsumerName, ProcessedAt = DateTimeOffset.UtcNow
            });
            try
            {
                await db.SaveChangesAsync(ct);
            }
            catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation })
            {
                logger.LogInformation("Duplicate event {MessageId} detected at commit", messageId);
            }

            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (DbException ex)
        {
            logger.LogWarning("Database unavailable, event {MessageId} returned to the queue: {Error}",
                ea.BasicProperties.MessageId, ex.Message);
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
            await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true, ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Event {MessageId} could not be processed and was dropped", ea.BasicProperties.MessageId);
            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ct);
        }
    }
}
