using DocFlow.Messaging;
using DocFlow.Persistence;
using DocFlow.Worker.Llm;

namespace DocFlow.Worker;

public static class WorkerServiceCollectionExtensions
{
    public static IServiceCollection AddWorker(this IServiceCollection services, IConfiguration configuration) =>
        services
            .AddRabbitMq(configuration)
            .AddPersistence(configuration)
            .AddLlmClient(configuration)
            .AddHostedService<AnalyzeDocumentConsumer>()
            .AddHostedService<OutboxRelay>();
}
