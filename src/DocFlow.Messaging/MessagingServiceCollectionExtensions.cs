using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace DocFlow.Messaging;

public static class MessagingServiceCollectionExtensions
{
    private const string ConnectionStringName = "RabbitMq";

    public static IServiceCollection AddRabbitMq(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString(ConnectionStringName)
            ?? throw new InvalidOperationException($"Connection string '{ConnectionStringName}' is not configured.");

        services.AddSingleton<IConnection>(_ =>
        {
            var factory = new ConnectionFactory
            {
                Uri = new Uri(connectionString),
                ClientProvidedName = AppDomain.CurrentDomain.FriendlyName
            };

            return factory.CreateConnectionAsync().GetAwaiter().GetResult();
        });
        services.AddSingleton<RabbitPublisher>();

        return services;
    }
}
