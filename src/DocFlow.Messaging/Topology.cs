using RabbitMQ.Client;

namespace DocFlow.Messaging;

public static class Topology
{
    public const string CommandsExchange = "docflow.commands";
    public const string AnalyzeQueue = "analyze-document";
    public const string AnalyzeRoutingKey = "analyze";

    public static async Task DeclareAsync(IChannel channel, CancellationToken ct = default)
    {
        await channel.ExchangeDeclareAsync(CommandsExchange, ExchangeType.Direct, durable: true, cancellationToken: ct);

        await channel.QueueDeclareAsync(AnalyzeQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" },
            cancellationToken: ct);

        await channel.QueueBindAsync(AnalyzeQueue, CommandsExchange, AnalyzeRoutingKey, cancellationToken: ct);
    }
}