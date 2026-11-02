using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace DocFlow.Messaging;

public static class TracingServiceCollectionExtensions
{
    private const string NpgsqlSourceName = "Npgsql";

    public static IServiceCollection AddTracing(
        this IServiceCollection services,
        string serviceName,
        Action<TracerProviderBuilder>? configure = null)
    {
        services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(serviceName))
            .WithTracing(tracing =>
            {
                tracing
                    .AddSource(Telemetry.SourceName)
                    .AddSource(NpgsqlSourceName)
                    .AddRabbitMQInstrumentation()
                    .AddOtlpExporter();

                configure?.Invoke(tracing);
            });

        return services;
    }
}
