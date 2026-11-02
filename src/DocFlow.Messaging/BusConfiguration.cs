using MassTransit;
using Microsoft.Extensions.Configuration;

namespace DocFlow.Messaging;

public static class BusConfiguration
{
    public const string AnalyzeDocumentQueue = "analyze-document";

    private const string ConnectionStringName = "RabbitMq";

    public static void UseDocFlowHost(this IRabbitMqBusFactoryConfigurator configurator, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString(ConnectionStringName)
            ?? throw new InvalidOperationException($"Connection string '{ConnectionStringName}' is not configured.");

        var uri = new Uri(connectionString);
        var credentials = uri.UserInfo.Split(':', 2);

        configurator.Host(uri.Host, (ushort)uri.Port, "/", host =>
        {
            host.Username(Uri.UnescapeDataString(credentials[0]));
            host.Password(Uri.UnescapeDataString(credentials[1]));
        });
    }
}
