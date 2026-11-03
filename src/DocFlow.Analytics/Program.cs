using DocFlow.Analytics;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddNpgsqlDataSource(builder.Configuration.GetConnectionString("DocFlow")!);
builder.Services.AddSingleton<AnalyticsStore>();
builder.Services.AddHostedService<AnalyticsConsumer>();

var host = builder.Build();
host.Services.GetRequiredService<AnalyticsStore>().EnsureSchema();
host.Run();
