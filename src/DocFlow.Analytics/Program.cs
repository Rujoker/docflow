using DocFlow.Analytics;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<AnalyticsConsumer>();

var host = builder.Build();
host.Run();
