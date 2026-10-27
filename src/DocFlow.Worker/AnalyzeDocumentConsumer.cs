using System.Text.Json;
using DocFlow.Contracts;
using DocFlow.Messaging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DocFlow.Worker;

public sealed class AnalyzeDocumentConsumer(IConnection connection, ILogger<AnalyzeDocumentConsumer> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        await Topology.DeclareAsync(channel, stoppingToken);

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            var command = JsonSerializer.Deserialize<AnalyzeDocument>(ea.Body.Span)!;
            logger.LogInformation("Start {DocumentId}, redelivered={Redelivered}", command.DocumentId, ea.Redelivered);

            await Task.Delay(TimeSpan.FromSeconds(10)); // LLM Call imitation, simulate processing time
            var text = await File.ReadAllTextAsync(command.StoragePath);
            logger.LogInformation("Done {DocumentId}: {Length} chars", command.DocumentId, text.Length);

            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        };

        await channel.BasicConsumeAsync(Topology.AnalyzeQueue, autoAck: false, consumer, stoppingToken);
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}