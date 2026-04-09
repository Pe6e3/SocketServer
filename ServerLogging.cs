using System.Text;

namespace SocketServer;

internal static class ServerLogging
{
    public static async Task BroadcastAsync(string message, CancellationToken cancellationToken = default)
    {
        await Console.Error.WriteLineAsync(message).ConfigureAwait(false);
        var line = $"{DateTime.Now:dd.MM HH:mm:ss.fff}\t{message}";
        await LogHub.BroadcastLineAsync(line, cancellationToken).ConfigureAwait(false);
    }
}

internal static class EndpointFormatter
{
    public static string Normalize(string endpoint)
    {
        if (string.IsNullOrWhiteSpace(endpoint))
            return endpoint;
        if (!endpoint.StartsWith("[::ffff:", StringComparison.OrdinalIgnoreCase))
            return endpoint;
        var endBracket = endpoint.IndexOf(']');
        if (endBracket < 0)
            return endpoint;
        var ip = endpoint[8..endBracket];
        var suffix = endpoint[(endBracket + 1)..];
        return $"{ip}{suffix}";
    }
}

internal static class DataPreview
{
    public static string Format(ReadOnlyMemory<byte> mem, int maxLen = 96)
    {
        var pool = new StringBuilder(Math.Min(mem.Length, maxLen) + 24);
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
