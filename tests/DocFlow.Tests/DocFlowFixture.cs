using DocFlow.Persistence;
using DocFlow.Worker;
using DocFlow.Worker.Llm;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;

namespace DocFlow.Tests;

public sealed class DocFlowFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder().WithImage("postgres:17").Build();
    private readonly RabbitMqContainer _rabbitMq = new RabbitMqBuilder().WithImage("rabbitmq:4-management").Build();

    public WebApplicationFactory<Program> Api { get; private set; } = null!;
    public IHost Worker { get; private set; } = null!;
    public CountingLlmClient Llm { get; } = new();
    public string RabbitMqConnectionString => _rabbitMq.GetConnectionString();

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_postgres.StartAsync(), _rabbitMq.StartAsync());

        Environment.SetEnvironmentVariable("ConnectionStrings__DocFlow", _postgres.GetConnectionString());
        Environment.SetEnvironmentVariable("ConnectionStrings__RabbitMq", _rabbitMq.GetConnectionString());
        Environment.SetEnvironmentVariable("Storage__Root", Path.Combine(Path.GetTempPath(), "docflow-tests"));

        Api = new WebApplicationFactory<Program>();
        await using (var scope = Api.Services.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<DocFlowDbContext>().Database.MigrateAsync();
        }

        var workerBuilder = Host.CreateApplicationBuilder();
        workerBuilder.Services.AddWorker(workerBuilder.Configuration);
        workerBuilder.Services.AddSingleton<ILlmClient>(Llm);
        Worker = workerBuilder.Build();
        await Worker.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await Worker.StopAsync();
        Worker.Dispose();
        await Api.DisposeAsync();
        await Task.WhenAll(_postgres.DisposeAsync().AsTask(), _rabbitMq.DisposeAsync().AsTask());
    }
}

[CollectionDefinition(nameof(DocFlowCollection))]
public sealed class DocFlowCollection : ICollectionFixture<DocFlowFixture>;
