using System.Text;
using DocFlow.Messaging;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DocFlow.Persistence;

public sealed class OutboxRelay(IServiceScopeFactory scopes, RabbitPublisher publisher,
    IConfiguration config, ILogger<OutboxRelay> logger) : BackgroundService
{
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
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }

    private async Task<int> PublishBatchAsync(CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DocFlowDbContext>();
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var batch = await db.OutboxMessages
            .FromSql($"SELECT * FROM outbox_messages WHERE processed_at IS NULL ORDER BY created_at LIMIT 20 FOR UPDATE SKIP LOCKED")
            .ToListAsync(ct);

        foreach (var message in batch)
        {
            await publisher.PublishRawAsync(message.Exchange, message.RoutingKey, message.Type,
                Encoding.UTF8.GetBytes(message.Payload), message.Id, ct);
            message.ProcessedAt = DateTimeOffset.UtcNow;

            if (config.GetValue<bool>("Chaos:CrashAfterPublish"))
                Environment.FailFast("Chaos: crashed after publish, before commit");
        }

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
        return batch.Count;
    }
}