using System.Text.Json;

namespace DocFlow.Persistence;

public sealed class OutboxMessage
{
    public Guid Id { get; set; }
    public required string Exchange { get; set; }
    public required string RoutingKey { get; set; }
    public required string Type { get; set; }
    public required string Payload { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? ProcessedAt { get; set; }

    public static OutboxMessage Create<T>(string exchange, string routingKey, T message) => new()
    {
        Id = Guid.NewGuid(),
        Exchange = exchange,
        RoutingKey = routingKey,
        Type = typeof(T).Name,
        Payload = JsonSerializer.Serialize(message),
        CreatedAt = DateTimeOffset.UtcNow
    };
}
