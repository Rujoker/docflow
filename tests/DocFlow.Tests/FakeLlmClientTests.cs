using DocFlow.Worker;
using DocFlow.Worker.Llm;

namespace DocFlow.Tests;

public sealed class FakeLlmClientTests
{
    private readonly FakeLlmClient _client = new();

    [Fact]
    public async Task Counts_lines_mentioning_third_parties()
    {
        const string text = """
            We share your data with third parties.
            We keep your data for as long as necessary.
            Third parties may contact you.
            """;

        var result = await _client.AnalyzeAsync(text, CancellationToken.None);

        Assert.Equal(2, result.IssuesFound);
    }

    [Fact]
    public async Task Poison_marker_is_a_permanent_error()
    {
        await Assert.ThrowsAsync<InvalidDocumentException>(
            () => _client.AnalyzeAsync("#poison", CancellationToken.None));
    }

    [Fact]
    public async Task Always_down_marker_is_a_transient_error()
    {
        await Assert.ThrowsAsync<LlmTransientException>(
            () => _client.AnalyzeAsync("#always-down", CancellationToken.None));
    }
}
