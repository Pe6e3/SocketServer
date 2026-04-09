using System.Globalization;
using System.Threading.Channels;
using Npgsql;

namespace SocketServer;

internal sealed class PostgresLogStore : IAsyncDisposable
{
    private readonly string _connectionString;
    private readonly Channel<LogWriteItem> _channel;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _worker;

    public PostgresLogStore(string connectionString)
    {
        _connectionString = connectionString;
        _channel = Channel.CreateUnbounded<LogWriteItem>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
        _worker = Task.Run(() => RunWorkerAsync(_cts.Token));
    }

    public async Task EnsureSchemaAsync(CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        const string ddl = """
create table if not exists log_entries (
    id bigserial primary key,
    created_at timestamptz not null,
    message text not null,
    event_type text null,
    remote text null
);
create index if not exists ix_log_entries_created_at on log_entries (created_at desc, id desc);
create index if not exists ix_log_entries_event_type on log_entries (event_type);
create index if not exists ix_log_entries_remote on log_entries (remote);
""";
        await using (var cmd = new NpgsqlCommand(ddl, conn))
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            await using var trigram = new NpgsqlCommand("create extension if not exists pg_trgm;", conn);
            await trigram.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await using var idx = new NpgsqlCommand(
                "create index if not exists ix_log_entries_message_trgm on log_entries using gin (message gin_trgm_ops);",
                conn);
            await idx.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Права на extension могут отсутствовать — таблица/поиск всё равно будут работать.
        }
    }

    public bool TryEnqueue(DateTimeOffset createdAt, string message)
    {
        var parsed = Parse(message);
        return _channel.Writer.TryWrite(new LogWriteItem(createdAt.UtcDateTime, message, parsed.EventType, parsed.Remote));
    }

    public async Task<IReadOnlyList<string>> QueryLinesAsync(
        DateTimeOffset? from,
        DateTimeOffset? to,
        string? q,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        const string sql = """
select created_at, message
from log_entries
where (@from is null or created_at >= @from)
  and (@to is null or created_at <= @to)
  and (@q is null or @q = '' or message ilike '%' || @q || '%')
order by created_at desc, id desc
limit @limit offset @offset;
""";

        var rows = new List<(DateTime createdAtUtc, string message)>(limit);
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("from", from?.UtcDateTime ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("to", to?.UtcDateTime ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("q", string.IsNullOrWhiteSpace(q) ? (object)DBNull.Value : q);
        cmd.Parameters.AddWithValue("limit", Math.Clamp(limit, 1, 1000));
        cmd.Parameters.AddWithValue("offset", Math.Max(offset, 0));
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            rows.Add((reader.GetDateTime(0), reader.GetString(1)));

        rows.Reverse();
        return rows
            .Select(row => $"{row.createdAtUtc.ToLocalTime():dd.MM HH:mm:ss.fff}\t{row.message}")
            .ToList();
    }

    private async Task RunWorkerAsync(CancellationToken cancellationToken)
    {
        var batch = new List<LogWriteItem>(256);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var first = await _channel.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                batch.Add(first);
                while (batch.Count < 256 && _channel.Reader.TryRead(out var item))
                    batch.Add(item);
                await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                batch.Clear();
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                await Task.Delay(200, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task FlushBatchAsync(List<LogWriteItem> batch, CancellationToken cancellationToken)
    {
        if (batch.Count == 0)
            return;

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        const string insertSql =
            "insert into log_entries (created_at, message, event_type, remote) values (@created_at, @message, @event_type, @remote);";
        await using var cmd = new NpgsqlCommand(insertSql, conn, tx);
        var pCreated = cmd.Parameters.Add("created_at", NpgsqlTypes.NpgsqlDbType.TimestampTz);
        var pMessage = cmd.Parameters.Add("message", NpgsqlTypes.NpgsqlDbType.Text);
        var pType = cmd.Parameters.Add("event_type", NpgsqlTypes.NpgsqlDbType.Text);
        var pRemote = cmd.Parameters.Add("remote", NpgsqlTypes.NpgsqlDbType.Text);

        foreach (var item in batch)
        {
            pCreated.Value = item.CreatedAtUtc;
            pMessage.Value = item.Message;
            pType.Value = item.EventType ?? (object)DBNull.Value;
            pRemote.Value = item.Remote ?? (object)DBNull.Value;
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static (string? EventType, string? Remote) Parse(string message)
    {
        if (!message.StartsWith('['))
            return (null, null);
        var close = message.IndexOf(']');
        if (close < 2)
            return (null, null);
        var eventType = message[1..close].Trim();
        var tail = message[(close + 1)..].TrimStart();
        if (string.IsNullOrEmpty(tail))
            return (eventType, null);

        if (tail.StartsWith('['))
        {
            var endRemote = tail.IndexOf(']');
            if (endRemote > 1)
                return (eventType, tail[1..endRemote]);
        }

        var firstToken = tail.Split(' ', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
        return (eventType, firstToken);
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        _cts.Cancel();
        try
        {
            await _worker.ConfigureAwait(false);
        }
        catch
        {
            // ignore on shutdown
        }
        _cts.Dispose();
    }

    private readonly record struct LogWriteItem(DateTime CreatedAtUtc, string Message, string? EventType, string? Remote);
}

internal static class LogPersistence
{
    private static PostgresLogStore? _store;

    public static bool Enabled => _store != null;

    public static async Task InitializeAsync(string? connectionString, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            return;
        var store = new PostgresLogStore(connectionString);
        await store.EnsureSchemaAsync(cancellationToken).ConfigureAwait(false);
        _store = store;
    }

    public static void TryEnqueue(DateTimeOffset createdAt, string message)
    {
        _store?.TryEnqueue(createdAt, message);
    }

    public static Task<IReadOnlyList<string>> QueryLinesAsync(
        DateTimeOffset? from,
        DateTimeOffset? to,
        string? q,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        if (_store == null)
            return Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());
        return _store.QueryLinesAsync(from, to, q, limit, offset, cancellationToken);
    }

    public static async Task ShutdownAsync()
    {
        if (_store == null)
            return;
        await _store.DisposeAsync().ConfigureAwait(false);
        _store = null;
    }
}
