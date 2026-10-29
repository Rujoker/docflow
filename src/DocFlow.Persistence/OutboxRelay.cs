using System.Text;
using DocFlow.Messaging;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DocFlow.Persistence;

public sealed class OutboxRelay(
    IServiceScopeFactory scopes,
    RabbitPublisher publisher,
    IConfiguration configuration,
    ILogger<OutboxRelay> logger) : BackgroundService
{
    private const int BatchSize = 20;
    private const string CrashAfterPublishKey = "Chaos:CrashAfterPublish";
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var published = 0;
            try
            {
                published = await PublishBatchAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning("Outbox relay failed, will retry: {Error}", ex.Message);
            }

            if (published == 0)
                await Task.Delay(PollInterval, stoppingToken);
        }
    }

    private async Task<int> PublishBatchAsync(CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DocFlowDbContext>();
        await using var transaction = await db.Database.BeginTransactionAsync(ct);

        var batch = await db.OutboxMessages
            .FromSql($"""
                SELECT * FROM outbox_messages
                WHERE processed_at IS NULL
                ORDER BY created_at
                LIMIT {BatchSize}
                FOR UPDATE SKIP LOCKED
                """)
            .ToListAsync(ct);

        foreach (var message in batch)
        {
            await publisher.PublishAsync(
                message.Exchange,
                message.RoutingKey,
                message.Type,
                Encoding.UTF8.GetBytes(message.Payload),
                message.Id,
                ct);
            message.ProcessedAt = DateTimeOffset.UtcNow;

            if (configuration.GetValue<bool>(CrashAfterPublishKey))
                Environment.FailFast("Chaos: crashed after publish, before commit");
        }

        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
        return batch.Count;
    }
}
