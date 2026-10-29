using Anthropic;
using Anthropic.Core;

namespace DocFlow.Worker.Llm;

public static class LlmServiceCollectionExtensions
{
    private const string ApiKeyConfigurationKey = "Anthropic:ApiKey";
    private const string ApiKeyEnvironmentVariable = "ANTHROPIC_API_KEY";
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(60);

    public static IServiceCollection AddLlmClient(this IServiceCollection services, IConfiguration configuration)
    {
        var options = configuration.GetSection(LlmOptions.SectionName).Get<LlmOptions>() ?? new LlmOptions();
        services.AddSingleton(options);

        if (options.IsFake)
            return services.AddSingleton<ILlmClient, FakeLlmClient>();

        if (options.IsClaude)
            return services.AddClaudeClient(configuration);

        throw new InvalidOperationException(
            $"Unknown {LlmOptions.SectionName}:Provider '{options.Provider}'. " +
            $"Supported values: '{LlmOptions.FakeProvider}', '{LlmOptions.ClaudeProvider}'.");
    }

    private static IServiceCollection AddClaudeClient(this IServiceCollection services, IConfiguration configuration)
    {
        var apiKey = configuration[ApiKeyConfigurationKey] ?? configuration[ApiKeyEnvironmentVariable];
        if (string.IsNullOrWhiteSpace(apiKey))
        {
            throw new InvalidOperationException(
                $"{LlmOptions.SectionName}:Provider is '{LlmOptions.ClaudeProvider}' but no API key is configured. " +
                $"Set the {ApiKeyEnvironmentVariable} environment variable or the {ApiKeyConfigurationKey} user secret.");
        }

        services.AddSingleton(new AnthropicClient(new ClientOptions
        {
            ApiKey = apiKey,
            MaxRetries = 0,
            Timeout = RequestTimeout
        }));

        return services.AddSingleton<ILlmClient, ClaudeLlmClient>();
    }
}
