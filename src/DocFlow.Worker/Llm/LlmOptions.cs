namespace DocFlow.Worker.Llm;

public sealed class LlmOptions
{
    public const string SectionName = "Llm";
    public const string FakeProvider = "Fake";
    public const string ClaudeProvider = "Claude";

    public string Provider { get; set; } = FakeProvider;
    public string Model { get; set; } = "claude-haiku-5-5";
    public int MaxTokens { get; set; } = 1024;

    public bool IsFake => Provider.Equals(FakeProvider, StringComparison.OrdinalIgnoreCase);
    public bool IsClaude => Provider.Equals(ClaudeProvider, StringComparison.OrdinalIgnoreCase);
}
