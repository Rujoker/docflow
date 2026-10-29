using RabbitMQ.Client;

namespace DocFlow.Messaging;

public sealed class RabbitPublisher(IConnection connection) : IAsyncDisposable
{
    private const string JsonContentType = "application/json";

    private const string ErrorTypeHeader = "error-type";
    private const string ErrorMessageHeader = "error-message";
    private const string AttemptsHeader = "attempts";
    private const string FailedAtHeader = "failed-at";

    private readonly SemaphoreSlim _lock = new(1, 1);
    private IChannel? _channel;

    public Task PublishAsync(
        string exchange,
        string routingKey,
        string messageType,
        ReadOnlyMemory<byte> body,
        Guid messageId,
        CancellationToken ct = default)
    {
        var properties = new BasicProperties
        {
            MessageId = messageId.ToString(),
            ContentType = JsonContentType,
            Type = messageType,
            Persistent = true
        };

        return PublishCoreAsync(exchange, routingKey, properties, body, ct);
    }

    public Task PublishToDeadLetterAsync(
        ReadOnlyMemory<byte> body,
        IReadOnlyBasicProperties original,
        Exception error,
        long attempts,
        CancellationToken ct = default)
    {
        var properties = new BasicProperties
        {
            MessageId = original.MessageId,
            ContentType = original.ContentType,
            Type = original.Type,
            Persistent = true,
            Headers = new Dictionary<string, object?>
            {
                [ErrorTypeHeader] = error.GetType().Name,
                [ErrorMessageHeader] = error.Message,
                [AttemptsHeader] = attempts,
                [FailedAtHeader] = DateTimeOffset.UtcNow.ToString("O")
            }
        };

        return PublishCoreAsync(Topology.DeadLetterExchange, routingKey: string.Empty, properties, body, ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
            await _channel.DisposeAsync();

        _lock.Dispose();
    }

    private async Task PublishCoreAsync(
        string exchange,
        string routingKey,
        BasicProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken ct)
    {
        await _lock.WaitAsync(ct);
        try
        {
            var channel = await GetChannelAsync(ct);
            await channel.BasicPublishAsync(
                exchange,
                routingKey,
                mandatory: true,
                basicProperties: properties,
                body: body,
                cancellationToken: ct);
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task<IChannel> GetChannelAsync(CancellationToken ct)
    {
        if (_channel is { IsClosed: false })
            return _channel;

        if (_channel is not null)
            await _channel.DisposeAsync();

        _channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true),
            ct);

        return _channel;
    }
}
