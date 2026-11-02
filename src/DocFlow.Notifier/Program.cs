using DocFlow.Messaging;
using DocFlow.Notifier;
using DocFlow.Persistence;
using MassTransit;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddPersistence(builder.Configuration);
builder.Services.AddMassTransit(bus =>
{
    bus.SetKebabCaseEndpointNameFormatter();
    bus.AddConsumer<DocumentAnalyzedNotificationConsumer>();
    bus.AddConsumer<DocumentFailedNotificationConsumer>();
    bus.AddEntityFrameworkOutbox<DocFlowDbContext>(outbox => outbox.UsePostgres());
    bus.AddConfigureEndpointsCallback((context, _, endpoint) =>
        endpoint.UseEntityFrameworkOutbox<DocFlowDbContext>(context));

    bus.UsingRabbitMq((context, rabbit) =>
    {
        rabbit.UseDocFlowHost(builder.Configuration);
        rabbit.ConfigureEndpoints(context);
    });
});

var host = builder.Build();
host.Run();
