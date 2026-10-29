using DocFlow.Messaging;
using DocFlow.Persistence;
using DocFlow.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddRabbit();
builder.Services.AddDocFlowPersistence(builder.Configuration);
builder.Services.AddLlmClient(builder.Configuration);
builder.Services.AddHostedService<AnalyzeDocumentConsumer>();
builder.Services.AddHostedService<OutboxRelay>();

var host = builder.Build();

var llm = host.Services.GetRequiredService<LlmOptions>();
host.Services.GetRequiredService<ILogger<Program>>()
    .LogInformation("LLM provider: {Provider}{Model}", llm.Provider,
        llm.Provider.Equals("Fake", StringComparison.OrdinalIgnoreCase) ? "" : $" ({llm.Model})");

host.Run();
