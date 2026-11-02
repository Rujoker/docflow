using System.Diagnostics;

namespace DocFlow.Messaging;

public static class Telemetry
{
    public const string SourceName = "DocFlow";

    public static readonly ActivitySource Source = new(SourceName);
}
