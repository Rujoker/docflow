using DocFlow.Messaging;
using DocFlow.Notifier;
using DocFlow.Persistence;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddRabbitMq(builder.Configuration);
builder.Services.AddPersistence(builder.Configuration);
builder.Services.AddHostedService<DocumentEventsConsumer>();
builder.Services.AddTracing("docflow-notifier");

var host = builder.Build();
host.Run();
