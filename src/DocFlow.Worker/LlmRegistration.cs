using Anthropic;
using Anthropic.Core;

namespace DocFlow.Worker;

public static class LlmRegistration
{
    public static IServiceCollection AddLlmClient(this IServiceCollection services, IConfiguration configuration)
    {
        var options = configuration.GetSection(LlmOptions.SectionName).Get<LlmOptions>() ?? new LlmOptions();
        services.AddSingleton(options);

        switch (options.Provider.ToLowerInvariant())
        {
            case "fake":
                services.AddSingleton<ILlmClient, FakeLlmClient>();
                break;

            case "claude":
                var apiKey = configuration["Anthropic:ApiKey"] ?? configuration["ANTHROPIC_API_KEY"];
                if (string.IsNullOrWhiteSpace(apiKey))
                    throw new InvalidOperationException(
                        "Llm:Provider is 'Claude' but no API key is configured. " +
                        "Set the ANTHROPIC_API_KEY environment variable or the Anthropic:ApiKey user secret.");

                services.AddSingleton(new AnthropicClient(new ClientOptions
                {
                    ApiKey = apiKey,
                    MaxRetries = 0,
                    Timeout = TimeSpan.FromSeconds(60)
                }));
                services.AddSingleton<ILlmClient, ClaudeLlmClient>();
                break;

            default:
                throw new InvalidOperationException(
                    $"Unknown Llm:Provider '{options.Provider}'. Supported values: 'Fake', 'Claude'.");
        }

        return services;
    }
}
