using System.Text;

namespace SocketServer;

internal static class ServerLogging
{
    public static async Task BroadcastAsync(string message, CancellationToken cancellationToken = default)
    {
        await Console.Error.WriteLineAsync(message).ConfigureAwait(false);
        var line = $"{DateTime.UtcNow:O}\t{message}";
        await LogHub.BroadcastLineAsync(line, cancellationToken).ConfigureAwait(false);
    }
}

internal static class DataPreview
{
    public static string Format(ReadOnlyMemory<byte> mem, int maxLen = 96)
    {
        var pool = new StringBuilder(Math.Min(mem.Length, maxLen) + 24);
        pool.Append('|');
        var span = mem.Span;
        var n = Math.Min(span.Length, maxLen);
        for (var i = 0; i < n; i++)
        {
            var b = span[i];
            if (b is >= 32 and < 127)
                pool.Append((char)b);
            else if (b == '\n')
                pool.Append("\\n");
            else if (b == '\r')
                pool.Append("\\r");
            else if (b == '\t')
                pool.Append("\\t");
            else
                pool.Append($"\\x{b:x2}");
        }

        if (span.Length > maxLen)
            pool.Append('…');
        return pool.ToString();
    }
}
