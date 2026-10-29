using RabbitMQ.Client;

namespace DocFlow.Messaging;

public static class Topology
{
    public const string CommandsExchange = "docflow.commands";
    public const string AnalyzeQueue = "analyze-document";
    public const string AnalyzeRoutingKey = "analyze";

    public const string RetryExchange = "docflow.retry";
    public const string RetryQueue = "analyze-document.retry";
    public static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(10);

    public const string DeadLetterExchange = "docflow.dlq";
    public const string DeadLetterQueue = "analyze-document.dlq";

    public const string EventsExchange = "docflow.events";
    public const string DocumentAnalyzedRoutingKey = "document.analyzed";
    public const string DocumentFailedRoutingKey = "document.failed";

    public const string NotifierQueue = "notifier.document-events";
    public const string NotifierBindingKey = "document.*";

    public static async Task DeclareAsync(IChannel channel, CancellationToken ct = default)
    {
        await channel.ExchangeDeclareAsync(CommandsExchange, ExchangeType.Direct, durable: true, cancellationToken: ct);
        await DeclareQuorumQueueAsync(channel, AnalyzeQueue, deadLetterExchange: RetryExchange, messageTtl: null, ct);
        await channel.QueueBindAsync(AnalyzeQueue, CommandsExchange, AnalyzeRoutingKey, cancellationToken: ct);

        await channel.ExchangeDeclareAsync(RetryExchange, ExchangeType.Fanout, durable: true, cancellationToken: ct);
        await DeclareQuorumQueueAsync(channel, RetryQueue, deadLetterExchange: CommandsExchange, messageTtl: RetryDelay, ct);
        await channel.QueueBindAsync(RetryQueue, RetryExchange, routingKey: string.Empty, cancellationToken: ct);

        await channel.ExchangeDeclareAsync(DeadLetterExchange, ExchangeType.Fanout, durable: true, cancellationToken: ct);
        await DeclareQuorumQueueAsync(channel, DeadLetterQueue, deadLetterExchange: null, messageTtl: null, ct);
        await channel.QueueBindAsync(DeadLetterQueue, DeadLetterExchange, routingKey: string.Empty, cancellationToken: ct);

        await DeclareEventsExchangeAsync(channel, ct);
    }

    public static async Task DeclareNotifierAsync(IChannel channel, CancellationToken ct = default)
    {
        await DeclareEventsExchangeAsync(channel, ct);
        await DeclareQuorumQueueAsync(channel, NotifierQueue, deadLetterExchange: null, messageTtl: null, ct);
        await channel.QueueBindAsync(NotifierQueue, EventsExchange, NotifierBindingKey, cancellationToken: ct);
    }

    private static Task DeclareEventsExchangeAsync(IChannel channel, CancellationToken ct) =>
        channel.ExchangeDeclareAsync(EventsExchange, ExchangeType.Topic, durable: true, cancellationToken: ct);

    private static Task DeclareQuorumQueueAsync(
        IChannel channel,
        string queue,
        string? deadLetterExchange,
        TimeSpan? messageTtl,
        CancellationToken ct)
    {
        var arguments = new Dictionary<string, object?> { ["x-queue-type"] = "quorum" };

        if (deadLetterExchange is not null)
            arguments["x-dead-letter-exchange"] = deadLetterExchange;

        if (messageTtl is not null)
            arguments["x-message-ttl"] = (int)messageTtl.Value.TotalMilliseconds;

        return channel.QueueDeclareAsync(
            queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: arguments,
            cancellationToken: ct);
    }
}
