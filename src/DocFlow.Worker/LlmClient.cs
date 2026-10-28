using DocFlow.Contracts;

namespace DocFlow.Worker;

public interface ILlmClient
{
    Task<AnalysisResult> AnalyzeAsync(string text, CancellationToken ct);
}

public sealed class LlmTransientException(string message) : Exception(message);
public sealed class InvalidDocumentException(string message) : Exception(message);

public sealed class FakeLlmClient : ILlmClient
{
    public async Task<AnalysisResult> AnalyzeAsync(string text, CancellationToken ct)
    {
        await Task.Delay(Random.Shared.Next(1000, 3000), ct);

        if (text.Contains("#poison"))
            throw new InvalidDocumentException("Document cannot be parsed");
        if (text.Contains("#always-down"))
            throw new LlmTransientException("503 Service Unavailable (simulated, never recovers)");
        if (text.Contains("#transient") && Random.Shared.NextDouble() < 0.7)
            throw new LlmTransientException("529 Overloaded (simulated)");

        var issues = text.Split('\n').Count(l => l.Contains("third part", StringComparison.OrdinalIgnoreCase));
        return new AnalysisResult(issues, $"Found {issues} potential issues");
    }
}