namespace DocFlow.Worker.Llm;

public interface ILlmClient
{
    Task<AnalysisResult> AnalyzeAsync(string text, CancellationToken ct);
}
