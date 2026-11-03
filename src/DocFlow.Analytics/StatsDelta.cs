namespace DocFlow.Analytics;

public readonly record struct StatsDelta(int Analyzed, int Failed, long IssuesFound)
{
    public static readonly StatsDelta None = new(0, 0, 0);
}
