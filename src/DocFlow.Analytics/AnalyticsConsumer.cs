using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using DocFlow.Contracts;

namespace DocFlow.Analytics;

public sealed class AnalyticsConsumer(IConfiguration configuration, ILogger<AnalyticsConsumer> logger)
    : BackgroundService
{
    private const string TopicName = "document-events";
    private const string MessageTypeHeader = "message-type";
    private const string DefaultGroupId = "analytics";
    private const int ReportEvery = 10;

    private int _analyzed;
    private int _failed;
    private long _issuesFound;

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);

    private void ConsumeLoop(CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = configuration.GetConnectionString("Kafka"),
            GroupId = configuration["Analytics:GroupId"] ?? DefaultGroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler((_, partitions) =>
                logger.LogInformation("Assigned partitions: {Partitions}", string.Join(",", partitions.Select(p => p.Partition.Value))))
            .SetPartitionsRevokedHandler((_, partitions) =>
                logger.LogInformation("Revoked partitions: {Partitions}", string.Join(",", partitions.Select(p => p.Partition.Value))))
            .Build();

        consumer.Subscribe(TopicName);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var record = consumer.Consume(ct);
                logger.LogInformation(
                    "P{Partition} offset {Offset} key {Key}",
                    record.Partition.Value, record.Offset.Value, record.Message.Key);

                Apply(record.Message);
                consumer.Commit(record);
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

    private void Apply(Message<string, string> message)
    {
        var type = message.Headers.TryGetLastBytes(MessageTypeHeader, out var bytes)
            ? Encoding.UTF8.GetString(bytes)
            : null;

        switch (type)
        {
            case nameof(DocumentAnalyzed):
                _analyzed++;
                _issuesFound += JsonSerializer.Deserialize<DocumentAnalyzed>(message.Value)!.IssuesFound;
                break;

            case nameof(DocumentFailed):
                _failed++;
                break;
        }

        if ((_analyzed + _failed) % ReportEvery == 0)
        {
            logger.LogInformation(
                "Stats: {Analyzed} analyzed, {Failed} failed, {IssuesFound} issues found",
                _analyzed, _failed, _issuesFound);
        }
    }
}
