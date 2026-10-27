using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace DocFlow.Messaging;

public static class RabbitSetup
{
    public static IServiceCollection AddRabbit(this IServiceCollection services, string host = "localhost")
    {
        services.AddSingleton<IConnection>(_ =>
        {
            var factory = new ConnectionFactory
            {
                HostName = host,
                ClientProvidedName = AppDomain.CurrentDomain.FriendlyName
            };
            return factory.CreateConnectionAsync().GetAwaiter().GetResult();
        });
        services.AddSingleton<RabbitPublisher>();
        return services;
    }
}