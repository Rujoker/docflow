using DocFlow.Messaging;
using DocFlow.Persistence;
using DocFlow.Worker;
using DocFlow.Worker.Llm;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddWorker(builder.Configuration);
var host = builder.Build();

var llmOptions = host.Services.GetRequiredService<LlmOptions>();
var logger = host.Services.GetRequiredService<ILogger<Program>>();
if (llmOptions.IsFake)
    logger.LogInformation("LLM provider: {Provider}", llmOptions.Provider);
else
    logger.LogInformation("LLM provider: {Provider} ({Model})", llmOptions.Provider, llmOptions.Model);

host.Run();
