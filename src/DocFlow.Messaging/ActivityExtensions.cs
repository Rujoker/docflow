using System.Diagnostics;

namespace DocFlow.Messaging;

public static class ActivityExtensions
{
    public static void RecordError(this Activity? activity, Exception error)
    {
        if (activity is null)
            return;

        activity.AddException(error);
        activity.SetStatus(ActivityStatusCode.Error, error.Message);
    }
}
