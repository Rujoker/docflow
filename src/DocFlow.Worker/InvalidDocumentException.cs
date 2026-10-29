namespace DocFlow.Worker;

public sealed class InvalidDocumentException(string message) : Exception(message);
