using System.Net;
using System.Text.Json;
using Anthropic;
using Anthropic.Exceptions;
using Anthropic.Models.Messages;
using DocFlow.Contracts;

namespace DocFlow.Worker;

public sealed class ClaudeLlmClient(AnthropicClient client, LlmOptions options) : ILlmClient
{
    private const string SystemPrompt = """
        You review legal documents such as privacy policies and terms of service.
        Find clauses that are risky for the user: sharing or selling personal data to third parties,
        open-ended data retention, waiving user rights, unilateral changes to the terms.
        Respond with JSON only, no other text:
        {"issuesFound": <number of risky clauses>, "summary": "<one or two sentences>"}
        """;

    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public async Task<AnalysisResult> AnalyzeAsync(string text, CancellationToken ct)
    {
        Message message;
        try
        {
            message = await client.Messages.Create(new MessageCreateParams
            {
                Model = options.Model,
                MaxTokens = options.MaxTokens,
                System = SystemPrompt,
                Messages = [new UserMessageParam { Content = text }]
            }, ct);
        }
        catch (Exception ex) when (IsTransient(ex))
        {
            throw new LlmTransientException($"Claude API transient failure: {ex.Message}");
        }
        catch (Exception ex) when (ex is AnthropicBadRequestException or AnthropicUnprocessableEntityException)
        {
            throw new InvalidDocumentException($"Claude API rejected the document: {ex.Message}");
        }

        var reply = string.Concat(message.Content.Select(block => block.TryPickText(out var t) ? t.Text : string.Empty));
        return Parse(reply);
    }

    private static bool IsTransient(Exception ex) =>
        ex is AnthropicRateLimitException or Anthropic5xxException or AnthropicIOException
        || ex is Anthropic4xxException { StatusCode: HttpStatusCode.RequestTimeout or HttpStatusCode.Conflict };

    private static AnalysisResult Parse(string reply)
    {
        var start = reply.IndexOf('{');
        var end = reply.LastIndexOf('}');
        if (start < 0 || end <= start)
            throw new LlmTransientException("Claude returned no JSON");

        try
        {
            return JsonSerializer.Deserialize<AnalysisResult>(reply[start..(end + 1)], JsonOptions)
                ?? throw new LlmTransientException("Claude returned empty JSON");
        }
        catch (JsonException ex)
        {
            throw new LlmTransientException($"Claude returned malformed JSON: {ex.Message}");
        }
    }
}
