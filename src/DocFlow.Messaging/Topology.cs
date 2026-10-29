using RabbitMQ.Client;

namespace DocFlow.Messaging;

public static class Topology
{
    public const string CommandsExchange = "docflow.commands";
    public const string AnalyzeQueue = "analyze-document";
    public const string AnalyzeRoutingKey = "analyze";
    public const string RetryExchange = "docflow.retry";
    public const string RetryQueue = "analyze-document.retry";
    public const string DeadLetterExchange = "docflow.dlq";
    public const string DeadLetterQueue = "analyze-document.dlq";
    public const string EventsTopic = "docflow.events";
    public const string DocumentAnalyzed = "document.analyzed";
    public const string DocumentFailed = "document.failed";
    public const string NotifierQueue = "notifier.document-events";
    public const string DocumentEventsPattern = "document.*";

    public const int MaxRetries = 3;

    public static async Task DeclareAsync(IChannel channel, CancellationToken ct = default)
    {
        await channel.ExchangeDeclareAsync(CommandsExchange, ExchangeType.Direct, durable: true, cancellationToken: ct);

        await channel.QueueDeclareAsync(AnalyzeQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-queue-type"] = "quorum",
                ["x-dead-letter-exchange"] = RetryExchange
            }, cancellationToken: ct);

        await channel.QueueBindAsync(AnalyzeQueue, CommandsExchange, AnalyzeRoutingKey, cancellationToken: ct);

        await channel.ExchangeDeclareAsync(RetryExchange, ExchangeType.Fanout, durable: true, cancellationToken: ct);

        await channel.QueueDeclareAsync(RetryQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-queue-type"] = "quorum",
                ["x-message-ttl"] = 10_000,
                ["x-dead-letter-exchange"] = CommandsExchange
            }, cancellationToken: ct);

        await channel.QueueBindAsync(RetryQueue, RetryExchange, routingKey: "", cancellationToken: ct);

        await channel.ExchangeDeclareAsync(DeadLetterExchange, ExchangeType.Fanout, durable: true, cancellationToken: ct);
        await channel.QueueDeclareAsync(DeadLetterQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" }, cancellationToken: ct);
        await channel.QueueBindAsync(DeadLetterQueue, DeadLetterExchange, routingKey: "", cancellationToken: ct);

        await channel.ExchangeDeclareAsync(EventsTopic, ExchangeType.Topic, durable: true, cancellationToken: ct);
    }
}