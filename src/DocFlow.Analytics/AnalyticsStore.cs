using Confluent.Kafka;
using Npgsql;

namespace DocFlow.Analytics;

public sealed class AnalyticsStore(NpgsqlDataSource dataSource)
{
    public void EnsureSchema()
    {
        using var command = dataSource.CreateCommand("""
            CREATE SCHEMA IF NOT EXISTS analytics;
            CREATE TABLE IF NOT EXISTS analytics.processed_events (
                message_id uuid PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS analytics.offsets (
                topic text NOT NULL,
                partition int NOT NULL,
                next_offset bigint NOT NULL,
                PRIMARY KEY (topic, partition));
            CREATE TABLE IF NOT EXISTS analytics.stats (
                id int PRIMARY KEY,
                analyzed bigint NOT NULL,
                failed bigint NOT NULL,
                issues_found bigint NOT NULL);
            INSERT INTO analytics.stats VALUES (1, 0, 0, 0) ON CONFLICT DO NOTHING;
            """);
        command.ExecuteNonQuery();
    }

    public Dictionary<(string Topic, int Partition), long> LoadOffsets()
    {
        using var command = dataSource.CreateCommand("SELECT topic, partition, next_offset FROM analytics.offsets");
        using var reader = command.ExecuteReader();

        var offsets = new Dictionary<(string, int), long>();
        while (reader.Read())
            offsets[(reader.GetString(0), reader.GetInt32(1))] = reader.GetInt64(2);

        return offsets;
    }

    public bool Record(TopicPartitionOffset position, Guid? messageId, StatsDelta delta)
    {
        using var connection = dataSource.OpenConnection();
        using var transaction = connection.BeginTransaction();

        var isNew = true;
        if (messageId is { } id)
        {
            using var dedup = new NpgsqlCommand(
                "INSERT INTO analytics.processed_events VALUES (@id) ON CONFLICT DO NOTHING", connection, transaction);
            dedup.Parameters.AddWithValue("id", id);
            isNew = dedup.ExecuteNonQuery() == 1;
        }

        if (isNew && delta != StatsDelta.None)
        {
            using var stats = new NpgsqlCommand("""
                UPDATE analytics.stats
                SET analyzed = analyzed + @analyzed,
                    failed = failed + @failed,
                    issues_found = issues_found + @issues
                WHERE id = 1
                """, connection, transaction);
            stats.Parameters.AddWithValue("analyzed", (long)delta.Analyzed);
            stats.Parameters.AddWithValue("failed", (long)delta.Failed);
            stats.Parameters.AddWithValue("issues", delta.IssuesFound);
            stats.ExecuteNonQuery();
        }

        using var offset = new NpgsqlCommand("""
            INSERT INTO analytics.offsets VALUES (@topic, @partition, @next)
            ON CONFLICT (topic, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset
            """, connection, transaction);
        offset.Parameters.AddWithValue("topic", position.Topic);
        offset.Parameters.AddWithValue("partition", position.Partition.Value);
        offset.Parameters.AddWithValue("next", position.Offset.Value + 1);
        offset.ExecuteNonQuery();

        transaction.Commit();
        return isNew;
    }

    public (long Analyzed, long Failed, long IssuesFound) ReadStats()
    {
        using var command = dataSource.CreateCommand("SELECT analyzed, failed, issues_found FROM analytics.stats WHERE id = 1");
        using var reader = command.ExecuteReader();
        reader.Read();
        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }
}
