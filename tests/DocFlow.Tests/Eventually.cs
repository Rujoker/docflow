namespace DocFlow.Tests;

public static class Eventually
{
    public static async Task<T> GetAsync<T>(Func<Task<T>> probe, Func<T, bool> isDone, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        while (true)
        {
            var value = await probe();
            if (isDone(value) || DateTime.UtcNow > deadline)
                return value;

            await Task.Delay(200);
        }
    }
}
