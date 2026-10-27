using DocFlow.Messaging;
using DocFlow.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddRabbit();
builder.Services.AddHostedService<AnalyzeDocumentConsumer>();

var host = builder.Build();
host.Run();
