using Confluent.Kafka;
using DocFlow.KafkaBridge;
using DocFlow.Messaging;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddRabbitMq(builder.Configuration);
builder.Services.AddSingleton<IProducer<string, string>>(_ =>
    new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = builder.Configuration.GetConnectionString("Kafka"),
        Acks = Acks.All,
        EnableIdempotence = true
    }).Build());
builder.Services.AddHostedService<KafkaBridgeConsumer>();

var host = builder.Build();
host.Run();
