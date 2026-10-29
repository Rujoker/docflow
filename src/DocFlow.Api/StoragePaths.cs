namespace DocFlow.Api;

public static class StoragePaths
{
    private const string SolutionFileName = "DocFlow.sln";

    public static string Resolve(string configured)
    {
        if (Path.IsPathRooted(configured))
            return configured;

        var root = FindRepositoryRoot() ?? Directory.GetCurrentDirectory();
        return Path.GetFullPath(Path.Combine(root, configured));
    }

    private static string? FindRepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, SolutionFileName)))
                return directory.FullName;
        }

        return null;
    }
}
