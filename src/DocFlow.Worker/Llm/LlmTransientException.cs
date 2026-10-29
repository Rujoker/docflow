namespace DocFlow.Worker.Llm;

public sealed class LlmTransientException(string message) : Exception(message);
