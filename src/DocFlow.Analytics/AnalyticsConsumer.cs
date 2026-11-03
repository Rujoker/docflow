using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using DocFlow.Contracts;

namespace DocFlow.Analytics;

public sealed class AnalyticsConsumer(
    IConfiguration configuration,
    AnalyticsStore store,
    ILogger<AnalyticsConsumer> logger) : BackgroundService
{
    private const string DefaultTopics = "document-events";
    private const string DeadLetterTopicName = "document-events.dlt";
    private const string MessageIdHeader = "message-id";
    private const string MessageTypeHeader = "message-type";
    private const string DefaultGroupId = "analytics";

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);

    private void ConsumeLoop(CancellationToken ct)
    {
        var bootstrapServers = configuration.GetConnectionString("Kafka");
        var topics = (configuration["Analytics:Topics"] ?? DefaultTopics)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = configuration["Analytics:GroupId"] ?? DefaultGroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler((_, partitions) => ResumeFromStore(partitions))
            .SetPartitionsRevokedHandler((_, partitions) =>
                logger.LogInformation("Revoked partitions: {Partitions}", string.Join(", ", partitions)))
            .Build();

        using var deadLetterProducer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();

        consumer.Subscribe(topics);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var record = consumer.Consume(ct);
                var messageId = ReadMessageId(record.Message);

                StatsDelta delta;
                try
                {
                    delta = ToDelta(record.Message);
                }
                catch (JsonException ex)
                {
                    SendToDeadLetter(deadLetterProducer, record, ex);
                    store.Record(record.TopicPartitionOffset, null, StatsDelta.None);
                    consumer.Commit(record);
                    continue;
                }

                var counted = store.Record(record.TopicPartitionOffset, messageId, delta);
                consumer.Commit(record);

                if (counted)
                {
                    var (analyzed, failed, issues) = store.ReadStats();
                    logger.LogInformation(
                        "{Topic} P{Partition} offset {Offset}: {Analyzed} analyzed, {Failed} failed, {Issues} issues",
                        record.Topic, record.Partition.Value, record.Offset.Value, analyzed, failed, issues);
                }
                else
                {
                    logger.LogInformation(
                        "{Topic} P{Partition} offset {Offset}: duplicate {MessageId} skipped",
                        record.Topic, record.Partition.Value, record.Offset.Value, messageId);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            consumer.Close();
        }
    }

    private IEnumerable<TopicPartitionOffset> ResumeFromStore(List<TopicPartition> partitions)
    {
        var stored = store.LoadOffsets();
        var resumed = partitions
            .Select(p => stored.TryGetValue((p.Topic, p.Partition.Value), out var next)
                ? new TopicPartitionOffset(p, new Offset(next))
                : new TopicPartitionOffset(p, Offset.Beginning))
            .ToList();

        logger.LogInformation("Assigned, resuming from store: {Positions}", string.Join(", ", resumed));
        return resumed;
    }

    private static Guid? ReadMessageId(Message<string, string> message) =>
        message.Headers.TryGetLastBytes(MessageIdHeader, out var bytes)
        && Guid.TryParse(Encoding.UTF8.GetString(bytes), out var id)
            ? id
            : null;

    private static StatsDelta ToDelta(Message<string, string> message)
    {
        var type = message.Headers.TryGetLastBytes(MessageTypeHeader, out var bytes)
            ? Encoding.UTF8.GetString(bytes)
            : null;

        return type switch
        {
            nameof(DocumentAnalyzed) => new StatsDelta(1, 0,
                JsonSerializer.Deserialize<DocumentAnalyzed>(message.Value)!.IssuesFound),
            nameof(DocumentFailed) => new StatsDelta(0, 1, 0),
            _ => StatsDelta.None
        };
    }

    private void SendToDeadLetter(IProducer<string, string> producer, ConsumeResult<string, string> record, Exception ex)
    {
        logger.LogError(ex, "Poison message at {Position}, sending to {Topic}",
            record.TopicPartitionOffset, DeadLetterTopicName);

        var headers = new Headers();
        foreach (var header in record.Message.Headers)
            headers.Add(header.Key, header.GetValueBytes());

        headers.Add("dlt-error", Encoding.UTF8.GetBytes(ex.Message));
        headers.Add("dlt-source", Encoding.UTF8.GetBytes(record.TopicPartitionOffset.ToString()));

        producer.ProduceAsync(DeadLetterTopicName, new Message<string, string>
        {
            Key = record.Message.Key,
            Value = record.Message.Value,
            Headers = headers
        }).GetAwaiter().GetResult();
    }
}
