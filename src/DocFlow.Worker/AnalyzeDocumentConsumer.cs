using System.Text;
using System.Text.Json;
using DocFlow.Contracts;
using DocFlow.Messaging;
using Polly;
using Polly.Retry;
using Polly.Timeout;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DocFlow.Worker;

public sealed class AnalyzeDocumentConsumer(
    IConnection connection,
    ILlmClient llm,
    RabbitPublisher publisher,
    ILogger<AnalyzeDocumentConsumer> logger) : BackgroundService
{
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
        try
        {
            var command = JsonSerializer.Deserialize<AnalyzeDocument>(ea.Body.Span)
                ?? throw new InvalidDocumentException("Empty message body");
            logger.LogInformation("Start {DocumentId}, attempt {Attempt}, redelivered={Redelivered}",
                command.DocumentId, attempt, ea.Redelivered);

            var text = await File.ReadAllTextAsync(command.StoragePath, ct);
            var result = await Resilience.ExecuteAsync(async token => await llm.AnalyzeAsync(text, token), ct);

            logger.LogInformation("Analyzed {DocumentId}: {Summary} (attempt {Attempt})",
                command.DocumentId, result.Summary, attempt);
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
            await publisher.PublishToDeadLetterAsync(ea.Body, ea.BasicProperties, ex, attempt, ct);
            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ct);
        }
    }

    private static bool IsTransient(Exception ex) => ex is LlmTransientException or TimeoutRejectedException;

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
