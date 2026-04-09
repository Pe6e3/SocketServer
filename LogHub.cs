using System.Net.Sockets;
using System.Text;

namespace SocketServer;

/// <summary>Подписчики на отдельном TCP-порту получают строки логов в UTF-8 с переводом строки.</summary>
internal static class LogHub
{
    private static readonly object Gate = new();
    private static readonly List<Stream> Subscribers = new();

    public static void AddSubscriber(Stream stream)
    {
        lock (Gate)
            Subscribers.Add(stream);
    }

    public static void RemoveSubscriber(Stream stream)
    {
        lock (Gate)
            Subscribers.Remove(stream);
    }

    public static async Task BroadcastLineAsync(string line, CancellationToken cancellationToken = default)
    {
        byte[] payload;
        lock (Gate)
        {
            if (Subscribers.Count == 0)
                return;
            payload = Encoding.UTF8.GetBytes(line + "\n");
        }

        List<Stream> snapshot;
        lock (Gate)
            snapshot = Subscribers.ToList();

        var dead = new List<Stream>();
        foreach (var stream in snapshot)
        {
            try
            {
                await stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                dead.Add(stream);
            }
        }

        if (dead.Count == 0)
            return;

        lock (Gate)
        {
            foreach (var s in dead)
                Subscribers.Remove(s);
        }
    }

    public static async Task AcceptLogClientsAsync(TcpListener logListener, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await logListener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                continue;
            }

            _ = HoldLogSubscriberAsync(client, cancellationToken);
        }
    }

    private static async Task HoldLogSubscriberAsync(TcpClient client, CancellationToken cancellationToken)
    {
        var remote = client.Client.RemoteEndPoint?.ToString() ?? "?";
        Stream? stream = null;
        try
        {
            stream = client.GetStream();
            LogHub.AddSubscriber(stream);
            await ServerLogging.BroadcastAsync($"[log-subscribe] {remote}", cancellationToken).ConfigureAwait(false);
            var buf = new byte[256];
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var n = await stream.ReadAsync(buf, cancellationToken).ConfigureAwait(false);
                    if (n == 0)
                        break;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (IOException)
                {
                    break;
                }
            }
        }
        finally
        {
            if (stream != null)
                LogHub.RemoveSubscriber(stream);
            await ServerLogging.BroadcastAsync($"[log-unsubscribe] {remote}", cancellationToken).ConfigureAwait(false);
            client.Dispose();
        }
    }
}
