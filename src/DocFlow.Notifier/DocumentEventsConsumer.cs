using System.Data.Common;
using System.Text.Json;
using DocFlow.Contracts;
using DocFlow.Messaging;
using DocFlow.Persistence;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DocFlow.Notifier;

public sealed class DocumentEventsConsumer(
    IConnection connection,
    IServiceScopeFactory scopes,
    ILogger<DocumentEventsConsumer> logger) : BackgroundService
{
    private const string ConsumerName = "notifier";
    private const ushort PrefetchCount = 10;
    private static readonly TimeSpan DatabaseRetryDelay = TimeSpan.FromSeconds(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        await Topology.DeclareNotifierAsync(channel, stoppingToken);
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: PrefetchCount, global: false, stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, delivery) => HandleAsync(channel, delivery, stoppingToken);

        await channel.BasicConsumeAsync(Topology.NotifierQueue, autoAck: false, consumer, stoppingToken);
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task HandleAsync(IChannel channel, BasicDeliverEventArgs delivery, CancellationToken ct)
    {
        try
        {
            if (Guid.TryParse(delivery.BasicProperties.MessageId, out var messageId))
                await NotifyAsync(messageId, delivery, ct);
            else
                logger.LogError("Event without a valid MessageId dropped");

            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (DbException ex)
        {
            logger.LogWarning(
                "Database unavailable, event {MessageId} returned to the queue: {Error}",
                delivery.BasicProperties.MessageId, ex.Message);
            await Task.Delay(DatabaseRetryDelay, ct);
            await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true, ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Event {MessageId} could not be processed and was dropped", delivery.BasicProperties.MessageId);
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, ct);
        }
    }

    private async Task NotifyAsync(Guid messageId, BasicDeliverEventArgs delivery, CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DocFlowDbContext>();

        if (await db.ProcessedMessages.IsProcessedAsync(messageId, ConsumerName, ct))
        {
            logger.LogInformation("Duplicate event {MessageId}, skipping", messageId);
            return;
        }

        SendNotification(messageId, delivery);

        db.ProcessedMessages.Add(ProcessedMessage.Create(messageId, ConsumerName));
        try
        {
            await db.SaveChangesAsync(ct);
        }
        catch (DbUpdateException ex) when (ex.IsUniqueViolation())
        {
            logger.LogInformation("Duplicate event {MessageId} detected at commit", messageId);
        }
    }

    private void SendNotification(Guid messageId, BasicDeliverEventArgs delivery)
    {
        switch (delivery.BasicProperties.Type)
        {
            case nameof(DocumentAnalyzed):
                var analyzed = JsonSerializer.Deserialize<DocumentAnalyzed>(delivery.Body.Span)!;
                logger.LogInformation(
                    "Email to user: document {DocumentId} analyzed, {IssuesFound} potential issues. {Summary}",
                    analyzed.DocumentId, analyzed.IssuesFound, analyzed.Summary);
                break;

            case nameof(DocumentFailed):
                var failed = JsonSerializer.Deserialize<DocumentFailed>(delivery.Body.Span)!;
                logger.LogInformation(
                    "Email to user: document {DocumentId} could not be analyzed: {Reason}",
                    failed.DocumentId, failed.Reason);
                break;

            default:
                logger.LogWarning(
                    "Unknown event type {Type}, message {MessageId} ignored",
                    delivery.BasicProperties.Type, messageId);
                break;
        }
    }
}
