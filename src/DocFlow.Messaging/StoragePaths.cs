namespace DocFlow.Messaging;

public static class StoragePaths
{
    public static string Resolve(string configured)
    {
        if (Path.IsPathRooted(configured))
            return configured;

        var root = FindRepoRoot() ?? Directory.GetCurrentDirectory();
        return Path.GetFullPath(Path.Combine(root, configured));
    }

    private static string? FindRepoRoot()
    {
        // bin/Debug/net10.0 → ... → корень репозитория
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            if (File.Exists(Path.Combine(dir.FullName, "DocFlow.sln")))
                return dir.FullName;
        }
        return null;
    }
}