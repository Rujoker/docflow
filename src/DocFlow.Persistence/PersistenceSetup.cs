using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DocFlow.Persistence;

public static class PersistenceSetup
{
    public static IServiceCollection AddDocFlowPersistence(this IServiceCollection services, IConfiguration configuration) =>
        services.AddDbContext<DocFlowDbContext>(o => o
            .UseNpgsql(configuration.GetConnectionString("DocFlow"))
            .UseSnakeCaseNamingConvention());
}
