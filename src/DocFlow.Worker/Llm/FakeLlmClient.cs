namespace DocFlow.Worker.Llm;

public sealed class FakeLlmClient : ILlmClient
{
    private const string PoisonMarker = "#poison";
    private const string AlwaysDownMarker = "#always-down";
    private const string TransientMarker = "#transient";
    private const double TransientFailureRate = 0.7;

    public async Task<AnalysisResult> AnalyzeAsync(string text, CancellationToken ct)
    {
        await Task.Delay(Random.Shared.Next(1000, 3000), ct);

        if (text.Contains(PoisonMarker))
            throw new InvalidDocumentException("Document cannot be parsed");

        if (text.Contains(AlwaysDownMarker))
            throw new LlmTransientException("503 Service Unavailable (simulated, never recovers)");

        if (text.Contains(TransientMarker) && Random.Shared.NextDouble() < TransientFailureRate)
            throw new LlmTransientException("529 Overloaded (simulated)");

        var issuesFound = text
            .Split('\n')
            .Count(line => line.Contains("third part", StringComparison.OrdinalIgnoreCase));

        return new AnalysisResult(issuesFound, $"Found {issuesFound} potential issues");
    }
}
