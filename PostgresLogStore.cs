using System.Globalization;
using System.Threading.Channels;
using Microsoft.Data.Sqlite;

namespace SocketServer;

internal sealed class SqliteLogStore : IAsyncDisposable
{
    private readonly string _dbPath;
    private readonly int _retentionDays;
    private readonly int _segmentMaxRows;
    private readonly long _maxTotalRows;
    private readonly int _maxSamePerSecond;
    private readonly Channel<LogWriteItem> _channel;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _worker;
    private readonly Dictionary<string, BurstCounter> _burstCounters = new(StringComparer.Ordinal);
    private SegmentState? _currentSegment;
    private long _droppedByQueue;

    public SqliteLogStore(
        string dbPath,
        int retentionDays,
        int segmentMaxRows,
        long maxTotalRows,
        int maxSamePerSecond)
    {
        _dbPath = dbPath;
        _retentionDays = Math.Max(1, retentionDays);
        _segmentMaxRows = Math.Max(10_000, segmentMaxRows);
        _maxTotalRows = Math.Max(100_000, maxTotalRows);
        _maxSamePerSecond = Math.Max(10, maxSamePerSecond);
        _channel = Channel.CreateBounded<LogWriteItem>(new BoundedChannelOptions(10_000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });
        _worker = Task.Run(() => RunWorkerAsync(_cts.Token));
    }

    public async Task EnsureSchemaAsync(CancellationToken cancellationToken)
    {
        var dir = Path.GetDirectoryName(_dbPath);
        if (!string.IsNullOrWhiteSpace(dir))
            Directory.CreateDirectory(dir);

        await using var conn = new SqliteConnection($"Data Source={_dbPath};Mode=ReadWriteCreate;Cache=Shared");
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using (var pragma = conn.CreateCommand())
        {
            pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;";
            await pragma.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        const string ddl = """
create table if not exists log_segments (
    id integer primary key autoincrement,
    day_key text not null,
    seq integer not null,
    first_ts text not null,
    last_ts text not null,
    row_count integer not null default 0,
    is_open integer not null default 1,
    unique(day_key, seq)
);
create table if not exists log_entries (
    id integer primary key autoincrement,
    segment_id integer not null references log_segments(id) on delete cascade,
    created_at text not null,
    message text not null,
    event_type text null,
    remote text null
);
create index if not exists ix_log_entries_segment on log_entries (segment_id);
create index if not exists ix_log_entries_created_at on log_entries (created_at desc, id desc);
create index if not exists ix_log_entries_event_type on log_entries (event_type);
create index if not exists ix_log_entries_remote on log_entries (remote);
create virtual table if not exists log_entries_fts using fts5(message, content='log_entries', content_rowid='id');
create trigger if not exists log_entries_ai after insert on log_entries begin
  insert into log_entries_fts(rowid, message) values (new.id, new.message);
end;
create trigger if not exists log_entries_ad after delete on log_entries begin
  insert into log_entries_fts(log_entries_fts, rowid, message) values ('delete', old.id, old.message);
end;
""";
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = ddl;
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public bool TryEnqueue(DateTimeOffset createdAt, string message)
    {
        if (!AllowMessage(createdAt.UtcDateTime, message, out var summary))
        {
            if (summary != null)
                _channel.Writer.TryWrite(new LogWriteItem(createdAt.UtcDateTime, summary, "log-throttle", null));
            return false;
        }
        var parsed = Parse(message);
        var ok = _channel.Writer.TryWrite(new LogWriteItem(createdAt.UtcDateTime, message, parsed.EventType, parsed.Remote));
        if (!ok)
            Interlocked.Increment(ref _droppedByQueue);
        return ok;
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
select e.created_at, e.message
from log_entries e
where (@from is null or created_at >= @from)
  and (@to is null or created_at <= @to)
  and (
      @q is null or @q = '' or
      e.message like '%' || @q || '%' collate nocase or
      e.id in (select rowid from log_entries_fts where log_entries_fts match @q)
  )
order by e.created_at desc, e.id desc
limit @limit offset @offset;
""";

        var rows = new List<(DateTime createdAtUtc, string message)>(limit);
        await using var conn = new SqliteConnection($"Data Source={_dbPath};Mode=ReadWriteCreate;Cache=Shared");
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("@from", from?.UtcDateTime.ToString("O", CultureInfo.InvariantCulture) ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@to", to?.UtcDateTime.ToString("O", CultureInfo.InvariantCulture) ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@q", string.IsNullOrWhiteSpace(q) ? (object)DBNull.Value : q!);
        cmd.Parameters.AddWithValue("@limit", Math.Clamp(limit, 1, 1000));
        cmd.Parameters.AddWithValue("@offset", Math.Max(offset, 0));
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            rows.Add((DateTime.Parse(reader.GetString(0), null, DateTimeStyles.RoundtripKind), reader.GetString(1)));

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
                while (batch.Count < 512 && _channel.Reader.TryRead(out var item))
                    batch.Add(item);
                var dropped = Interlocked.Exchange(ref _droppedByQueue, 0);
                if (dropped > 0)
                    batch.Add(new LogWriteItem(DateTime.UtcNow, $"[log-drop] queue overflow dropped {dropped} log entries", "log-drop", null));
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

        await using var conn = new SqliteConnection($"Data Source={_dbPath};Mode=ReadWriteCreate;Cache=Shared");
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var txBase = await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var tx = (SqliteTransaction)txBase;
        await using var insert = conn.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText =
            "insert into log_entries (segment_id, created_at, message, event_type, remote) values (@segment_id, @created_at, @message, @event_type, @remote);";
        insert.Parameters.AddWithValue("@segment_id", 0);
        insert.Parameters.AddWithValue("@created_at", "");
        insert.Parameters.AddWithValue("@message", "");
        insert.Parameters.AddWithValue("@event_type", (object)DBNull.Value);
        insert.Parameters.AddWithValue("@remote", (object)DBNull.Value);

        foreach (var item in batch)
        {
            var seg = await EnsureSegmentAsync(conn, tx, item.CreatedAtUtc, cancellationToken).ConfigureAwait(false);
            insert.Parameters["@segment_id"].Value = seg.Id;
            insert.Parameters["@created_at"].Value = item.CreatedAtUtc.ToString("O", CultureInfo.InvariantCulture);
            insert.Parameters["@message"].Value = item.Message;
            insert.Parameters["@event_type"].Value = item.EventType ?? (object)DBNull.Value;
            insert.Parameters["@remote"].Value = item.Remote ?? (object)DBNull.Value;
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            seg.RowCount++;
            seg.LastTsUtc = item.CreatedAtUtc;
        }

        await FinalizeOpenSegmentAsync(conn, tx, cancellationToken).ConfigureAwait(false);
        await PruneAsync(conn, tx, cancellationToken).ConfigureAwait(false);
        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<SegmentState> EnsureSegmentAsync(SqliteConnection conn, SqliteTransaction tx, DateTime tsUtc, CancellationToken ct)
    {
        var dayKey = tsUtc.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        if (_currentSegment != null)
        {
            var shouldRotate = _currentSegment.DayKey != dayKey || _currentSegment.RowCount >= _segmentMaxRows;
            if (!shouldRotate)
                return _currentSegment;

            await using var close = conn.CreateCommand();
            close.Transaction = tx;
            close.CommandText = "update log_segments set is_open=0, last_ts=@last_ts, row_count=@row_count where id=@id;";
            close.Parameters.AddWithValue("@last_ts", _currentSegment.LastTsUtc.ToString("O", CultureInfo.InvariantCulture));
            close.Parameters.AddWithValue("@row_count", _currentSegment.RowCount);
            close.Parameters.AddWithValue("@id", _currentSegment.Id);
            await close.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            _currentSegment = null;
        }

        await using var seqCmd = conn.CreateCommand();
        seqCmd.Transaction = tx;
        seqCmd.CommandText = "select coalesce(max(seq), 0) + 1 from log_segments where day_key=@day_key;";
        seqCmd.Parameters.AddWithValue("@day_key", dayKey);
        var seq = Convert.ToInt32(await seqCmd.ExecuteScalarAsync(ct).ConfigureAwait(false), CultureInfo.InvariantCulture);

        await using var create = conn.CreateCommand();
        create.Transaction = tx;
        create.CommandText = """
insert into log_segments (day_key, seq, first_ts, last_ts, row_count, is_open)
values (@day_key, @seq, @first_ts, @last_ts, 0, 1);
select last_insert_rowid();
""";
        create.Parameters.AddWithValue("@day_key", dayKey);
        create.Parameters.AddWithValue("@seq", seq);
        create.Parameters.AddWithValue("@first_ts", tsUtc.ToString("O", CultureInfo.InvariantCulture));
        create.Parameters.AddWithValue("@last_ts", tsUtc.ToString("O", CultureInfo.InvariantCulture));
        var id = Convert.ToInt64(await create.ExecuteScalarAsync(ct).ConfigureAwait(false), CultureInfo.InvariantCulture);
        _currentSegment = new SegmentState(id, dayKey, tsUtc, tsUtc, 0);
        return _currentSegment;
    }

    private async Task FinalizeOpenSegmentAsync(SqliteConnection conn, SqliteTransaction tx, CancellationToken ct)
    {
        if (_currentSegment == null)
            return;
        await using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "update log_segments set last_ts=@last_ts, row_count=@row_count where id=@id;";
        cmd.Parameters.AddWithValue("@last_ts", _currentSegment.LastTsUtc.ToString("O", CultureInfo.InvariantCulture));
        cmd.Parameters.AddWithValue("@row_count", _currentSegment.RowCount);
        cmd.Parameters.AddWithValue("@id", _currentSegment.Id);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task PruneAsync(SqliteConnection conn, SqliteTransaction tx, CancellationToken ct)
    {
        var cutoff = DateTime.UtcNow.AddDays(-_retentionDays).ToString("O", CultureInfo.InvariantCulture);
        await using (var delOld = conn.CreateCommand())
        {
            delOld.Transaction = tx;
            delOld.CommandText = "delete from log_segments where last_ts < @cutoff and is_open=0;";
            delOld.Parameters.AddWithValue("@cutoff", cutoff);
            await delOld.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using var totalCmd = conn.CreateCommand();
        totalCmd.Transaction = tx;
        totalCmd.CommandText = "select coalesce(sum(row_count),0) from log_segments;";
        var total = Convert.ToInt64(await totalCmd.ExecuteScalarAsync(ct).ConfigureAwait(false), CultureInfo.InvariantCulture);
        while (total > _maxTotalRows)
        {
            await using var oldest = conn.CreateCommand();
            oldest.Transaction = tx;
            oldest.CommandText = "select id, row_count from log_segments where is_open=0 order by last_ts asc limit 1;";
            await using var reader = await oldest.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                break;
            var id = reader.GetInt64(0);
            var count = reader.GetInt64(1);
            reader.Close();

            await using var del = conn.CreateCommand();
            del.Transaction = tx;
            del.CommandText = "delete from log_segments where id=@id;";
            del.Parameters.AddWithValue("@id", id);
            await del.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            total -= count;
        }
    }

    private bool AllowMessage(DateTime tsUtc, string message, out string? summary)
    {
        summary = null;
        var key = message.Length <= 160 ? message : message[..160];
        var second = new DateTime(tsUtc.Year, tsUtc.Month, tsUtc.Day, tsUtc.Hour, tsUtc.Minute, tsUtc.Second, DateTimeKind.Utc);
        if (!_burstCounters.TryGetValue(key, out var counter))
        {
            _burstCounters[key] = new BurstCounter(second, 1, 0);
            return true;
        }

        if (counter.WindowStartUtc != second)
        {
            if (counter.Suppressed > 0)
                summary = $"[log-throttle] suppressed {counter.Suppressed} similar messages in previous second";
            _burstCounters[key] = new BurstCounter(second, 1, 0);
            return true;
        }

        if (counter.Seen < _maxSamePerSecond)
        {
            _burstCounters[key] = counter with { Seen = counter.Seen + 1 };
            return true;
        }

        _burstCounters[key] = counter with { Suppressed = counter.Suppressed + 1 };
        return false;
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
    private sealed class SegmentState
    {
        public SegmentState(long id, string dayKey, DateTime firstTsUtc, DateTime lastTsUtc, int rowCount)
        {
            Id = id;
            DayKey = dayKey;
            FirstTsUtc = firstTsUtc;
            LastTsUtc = lastTsUtc;
            RowCount = rowCount;
        }
        public long Id { get; }
        public string DayKey { get; }
        public DateTime FirstTsUtc { get; }
        public DateTime LastTsUtc { get; set; }
        public int RowCount { get; set; }
    }
    private readonly record struct BurstCounter(DateTime WindowStartUtc, int Seen, int Suppressed);
}

internal static class LogPersistence
{
    private static SqliteLogStore? _store;

    public static bool Enabled => _store != null;

    public static async Task InitializeAsync(
        string? sqlitePath,
        int retentionDays,
        int segmentMaxRows,
        long maxTotalRows,
        int maxSamePerSecond,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(sqlitePath))
            return;
        try
        {
            var store = new SqliteLogStore(sqlitePath, retentionDays, segmentMaxRows, maxTotalRows, maxSamePerSecond);
            await store.EnsureSchemaAsync(cancellationToken).ConfigureAwait(false);
            _store = store;
        }
        catch (Exception ex)
        {
            await Console.Error.WriteLineAsync($"[log-persistence-disabled] {ex.GetType().Name}: {ex.Message}")
                .ConfigureAwait(false);
            _store = null;
        }
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
