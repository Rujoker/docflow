using DocFlow.Messaging;
using DocFlow.Persistence;
using DocFlow.Worker;
using DocFlow.Worker.Llm;
using MassTransit;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddPersistence(builder.Configuration);
builder.Services.AddLlmClient(builder.Configuration);
builder.Services.AddMassTransit(bus =>
{
    bus.SetKebabCaseEndpointNameFormatter();
    bus.AddConsumer<AnalyzeDocumentConsumer>();
    bus.AddConsumer<AnalyzeDocumentFaultConsumer>();
    bus.AddEntityFrameworkOutbox<DocFlowDbContext>(outbox => outbox.UsePostgres());

    bus.AddConfigureEndpointsCallback((context, _, endpoint) =>
    {
        endpoint.UseDelayedRedelivery(redelivery =>
        {
            redelivery.Intervals(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));
            redelivery.Ignore<InvalidDocumentException>();
        });
        endpoint.UseMessageRetry(retry =>
        {
            retry.Intervals(300, 1000);
            retry.Ignore<InvalidDocumentException>();
        });
        endpoint.UseEntityFrameworkOutbox<DocFlowDbContext>(context);
    });

    bus.UsingRabbitMq((context, rabbit) =>
    {
        rabbit.UseDocFlowHost(builder.Configuration);
        rabbit.UseDelayedMessageScheduler();
        rabbit.ConfigureEndpoints(context);
    });
});

var host = builder.Build();

var llmOptions = host.Services.GetRequiredService<LlmOptions>();
var logger = host.Services.GetRequiredService<ILogger<Program>>();
if (llmOptions.IsFake)
    logger.LogInformation("LLM provider: {Provider}", llmOptions.Provider);
else
    logger.LogInformation("LLM provider: {Provider} ({Model})", llmOptions.Provider, llmOptions.Model);

host.Run();
