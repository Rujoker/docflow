using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using DocFlow.Messaging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DocFlow.KafkaBridge;

public sealed class KafkaBridgeConsumer(
    IConnection connection,
    IProducer<string, string> producer,
    ILogger<KafkaBridgeConsumer> logger) : BackgroundService
{
    public const string TopicName = "document-events";
    public const string MessageIdHeader = "message-id";
    public const string MessageTypeHeader = "message-type";

    private const ushort PrefetchCount = 10;
    private static readonly TimeSpan KafkaRetryDelay = TimeSpan.FromSeconds(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        await Topology.DeclareKafkaBridgeAsync(channel, stoppingToken);
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: PrefetchCount, global: false, stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, delivery) => ForwardAsync(channel, delivery, stoppingToken);

        await channel.BasicConsumeAsync(Topology.KafkaBridgeQueue, autoAck: false, consumer, stoppingToken);
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task ForwardAsync(IChannel channel, BasicDeliverEventArgs delivery, CancellationToken ct)
    {
        try
        {
            using var json = JsonDocument.Parse(delivery.Body);
            var documentId = json.RootElement.GetProperty("DocumentId").GetString()!;

            var message = new Message<string, string>
            {
                Key = documentId,
                Value = Encoding.UTF8.GetString(delivery.Body.Span),
                Headers = new Confluent.Kafka.Headers
                {
                    { MessageIdHeader, Encoding.UTF8.GetBytes(delivery.BasicProperties.MessageId ?? string.Empty) },
                    { MessageTypeHeader, Encoding.UTF8.GetBytes(delivery.BasicProperties.Type ?? string.Empty) }
                }
            };

            await producer.ProduceAsync(TopicName, message, ct);
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            logger.LogWarning("Could not forward {MessageId} to Kafka, will retry: {Error}",
                delivery.BasicProperties.MessageId, ex.Message);
            await Task.Delay(KafkaRetryDelay, ct);
            await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true, ct);
        }
    }
}
