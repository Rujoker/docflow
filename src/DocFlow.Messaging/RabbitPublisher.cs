using System.Text.Json;
using RabbitMQ.Client;

namespace DocFlow.Messaging;

public sealed class RabbitPublisher(IConnection connection) : IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private IChannel? _channel;

    public async Task PublishAsync<T>(string exchange, string routingKey, T message, Guid messageId,
        CancellationToken ct = default)
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

            var props = new BasicProperties
            {
                MessageId = messageId.ToString(),
                ContentType = "application/json",
                Type = typeof(T).Name,
                Persistent = true
            };

            await _channel.BasicPublishAsync(exchange, routingKey, mandatory: true,
                basicProperties: props, body: JsonSerializer.SerializeToUtf8Bytes(message),
                cancellationToken: ct);
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
