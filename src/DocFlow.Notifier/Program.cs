using DocFlow.Messaging;
using DocFlow.Notifier;
using DocFlow.Persistence;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddRabbitMq(builder.Configuration);
builder.Services.AddPersistence(builder.Configuration);
builder.Services.AddHostedService<DocumentEventsConsumer>();

var host = builder.Build();
host.Run();
