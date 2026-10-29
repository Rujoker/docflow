using DocFlow.Messaging;
using DocFlow.Notifier;
using DocFlow.Persistence;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddRabbit();
builder.Services.AddDocFlowPersistence(builder.Configuration);
builder.Services.AddHostedService<DocumentEventsConsumer>();

var host = builder.Build();
host.Run();
