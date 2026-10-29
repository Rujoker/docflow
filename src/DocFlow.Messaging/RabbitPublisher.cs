using System.Text.Json;
using RabbitMQ.Client;

namespace DocFlow.Messaging;

public sealed class RabbitPublisher(IConnection connection) : IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private IChannel? _channel;

    public Task PublishAsync<T>(string exchange, string routingKey, T message, Guid messageId,
        CancellationToken ct = default)
    {
        var props = new BasicProperties
        {
            MessageId = messageId.ToString(),
            ContentType = "application/json",
            Type = typeof(T).Name,
            Persistent = true
        };

        return PublishCoreAsync(exchange, routingKey, props, JsonSerializer.SerializeToUtf8Bytes(message), ct);
    }
    
    public Task PublishRawAsync(string exchange, string routingKey, string type, ReadOnlyMemory<byte> body,
        Guid messageId, CancellationToken ct = default)
    {
        var props = new BasicProperties
        {
            MessageId = messageId.ToString(),
            ContentType = "application/json",
            Type = type,
            Persistent = true
        };
    
        return PublishCoreAsync(exchange, routingKey, props, body, ct);
    }

    public Task PublishToDeadLetterAsync(ReadOnlyMemory<byte> body, IReadOnlyBasicProperties original,
        Exception error, long attempts, CancellationToken ct = default)
    {
        var props = new BasicProperties
        {
            MessageId = original.MessageId,
            ContentType = original.ContentType,
            Type = original.Type,
            Persistent = true,
            Headers = new Dictionary<string, object?>
            {
                ["error-type"] = error.GetType().Name,
                ["error-message"] = error.Message,
                ["attempts"] = attempts,
                ["failed-at"] = DateTimeOffset.UtcNow.ToString("O")
            }
        };

        return PublishCoreAsync(Topology.DeadLetterExchange, routingKey: "", props, body, ct);
    }

    private async Task PublishCoreAsync(string exchange, string routingKey, BasicProperties props,
        ReadOnlyMemory<byte> body, CancellationToken ct)
    {
        await _lock.WaitAsync(ct);
        try
        {
            if (_channel is null || _channel.IsClosed)
            {
                if (_channel is not null) await _channel.DisposeAsync();
                _channel = await connection.CreateChannelAsync(
                    new CreateChannelOptions(
                        publisherConfirmationsEnabled: true,
                        publisherConfirmationTrackingEnabled: true),
                    ct);
            }

            await _channel.BasicPublishAsync(exchange, routingKey, mandatory: true,
                basicProperties: props, body: body, cancellationToken: ct);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null) await _channel.DisposeAsync();
    }
}
