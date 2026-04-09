using System.Net.Sockets;
using SocketServer;

var port = int.TryParse(Environment.GetEnvironmentVariable("PORT"), out var p) ? p : 1984;
var logPort = int.TryParse(Environment.GetEnvironmentVariable("LOG_PORT"), out var lp) ? lp : port + 1;
var loggerHttpPort = int.TryParse(Environment.GetEnvironmentVariable("LOGGER_HTTP_PORT"), out var hp) ? hp : 5080;
var sqlitePath =
    Environment.GetEnvironmentVariable("LOG_SQLITE_PATH") ??
    Environment.GetEnvironmentVariable("LOGS_SQLITE_PATH") ??
    "/home/pe6e3/SocketServer/data/logs.db";
var logRetentionDays =
    int.TryParse(Environment.GetEnvironmentVariable("LOG_RETENTION_DAYS"), out var retentionDays) ? retentionDays : 50;
var logSegmentMaxRows =
    int.TryParse(Environment.GetEnvironmentVariable("LOG_SEGMENT_MAX_ROWS"), out var segmentMaxRows) ? segmentMaxRows : 500_000;
var logMaxTotalRows =
    long.TryParse(Environment.GetEnvironmentVariable("LOG_MAX_TOTAL_ROWS"), out var maxTotalRows) ? maxTotalRows : 5_000_000;
var logMaxSamePerSecond =
    int.TryParse(Environment.GetEnvironmentVariable("LOG_MAX_SAME_PER_SECOND"), out var maxSamePerSecond) ? maxSamePerSecond : 200;

using var shutdown = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    shutdown.Cancel();
};

await LogPersistence.InitializeAsync(
        sqlitePath,
        logRetentionDays,
        logSegmentMaxRows,
        logMaxTotalRows,
        logMaxSamePerSecond,
        shutdown.Token)
    .ConfigureAwait(false);

var dataListener = TcpListener.Create(port);
var logListener = TcpListener.Create(logPort);
dataListener.Start();
logListener.Start();
var loggerHttpTask = SocketLoggerHttpServer.RunAsync(loggerHttpPort, logPort, shutdown.Token);

await ServerLogging.BroadcastAsync($"TCP *:{port} — приём данных (stdout процесса); логи *:{logPort} (подписчики); HTTP UI 127.0.0.1:{loggerHttpPort}", shutdown.Token).ConfigureAwait(false);

var logAcceptTask = LogHub.AcceptLogClientsAsync(logListener, shutdown.Token);

try
{
    while (!shutdown.IsCancellationRequested)
    {
        TcpClient client;
        try
        {
            client = await dataListener.AcceptTcpClientAsync(shutdown.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            break;
        }

        _ = HandleDataClientAsync(client, shutdown.Token);
    }
}
finally
{
    dataListener.Stop();
    logListener.Stop();
    await LogPersistence.ShutdownAsync().ConfigureAwait(false);
}

try
{
    await logAcceptTask.ConfigureAwait(false);
}
catch (OperationCanceledException)
{
    // завершение
}

try
{
    await loggerHttpTask.ConfigureAwait(false);
}
catch (OperationCanceledException)
{
    // завершение
}

static async Task HandleDataClientAsync(TcpClient client, CancellationToken cancellationToken)
{
    var remoteRaw = client.Client.RemoteEndPoint?.ToString() ?? "?";
    var remote = EndpointFormatter.Normalize(remoteRaw);
    await ServerLogging.BroadcastAsync($"[connect] {remote}", cancellationToken).ConfigureAwait(false);

    try
    {
        await using var stream = client.GetStream();
        var stdout = Console.OpenStandardOutput();
        var buffer = new byte[8192];
        int n;
        while (!cancellationToken.IsCancellationRequested &&
               (n = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken).ConfigureAwait(false)) > 0)
        {
            await stdout.WriteAsync(buffer.AsMemory(0, n), cancellationToken).ConfigureAwait(false);
            await stdout.FlushAsync(cancellationToken).ConfigureAwait(false);
            var preview = DataPreview.Format(buffer.AsMemory(0, n));
            await ServerLogging.BroadcastAsync($"[rx] [{remote}] ({n}B) {preview}", cancellationToken).ConfigureAwait(false);
        }
    }
    catch (IOException ex)
    {
        await ServerLogging.BroadcastAsync($"[socket error] {remote}: {ex.Message}", CancellationToken.None)
            .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
        // shutdown
    }
    catch (ObjectDisposedException)
    {
        // клиент закрыл соединение
    }
    finally
    {
        client.Dispose();
        await ServerLogging.BroadcastAsync($"[disconnect] {remote}", CancellationToken.None).ConfigureAwait(false);
    }
}
