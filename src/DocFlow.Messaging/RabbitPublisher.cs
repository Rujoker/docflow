using System.Text.Json;
using RabbitMQ.Client;

namespace DocFlow.Messaging;

public sealed class RabbitPublisher(IConnection connection) : IAsyncDisposable
{
    // Channel не потокобезопасен, а publisher — singleton, в который одновременно
    // ходят разные HTTP-запросы. Поэтому сериализуем доступ к каналу.
    private readonly SemaphoreSlim _lock = new(1, 1);
    private IChannel? _channel;

    public async Task PublishAsync<T>(string exchange, string routingKey, T message, Guid messageId,
        CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            // Канал, закрытый брокером (например, после ошибки протокола), сам не восстановится — создаём новый.
            if (_channel is null || _channel.IsClosed)
            {
                if (_channel is not null) await _channel.DisposeAsync();
                _channel = await connection.CreateChannelAsync(
                    new CreateChannelOptions(
                        publisherConfirmationsEnabled: true,          // брокер подтверждает каждое сообщение
                        publisherConfirmationTrackingEnabled: true),  // BasicPublishAsync ждёт подтверждения
                    ct);
            }

            var props = new BasicProperties
            {
                MessageId = messageId.ToString(),
                ContentType = "application/json",
                Type = typeof(T).Name,
                Persistent = true // DeliveryMode = 2
            };

            // Завершается только после ack от брокера. При nack или возврате
            // немаршрутизируемого сообщения (mandatory) бросает PublishException.
            await _channel.BasicPublishAsync(exchange, routingKey,
                mandatory: true, // вернуть, если ни одна очередь не подошла
                basicProperties: props, body: JsonSerializer.SerializeToUtf8Bytes(message), cancellationToken: ct);
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
