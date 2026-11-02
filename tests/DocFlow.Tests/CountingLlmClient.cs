using System.Collections.Concurrent;
using DocFlow.Worker.Llm;

namespace DocFlow.Tests;

public sealed class CountingLlmClient : ILlmClient
{
    private readonly FakeLlmClient _inner = new();
    private readonly ConcurrentDictionary<string, int> _calls = new();

    public Task<AnalysisResult> AnalyzeAsync(string text, CancellationToken ct)
    {
        _calls.AddOrUpdate(text, 1, (_, count) => count + 1);
        return _inner.AnalyzeAsync(text, ct);
    }

    public int CallsFor(string text) => _calls.GetValueOrDefault(text);
}
