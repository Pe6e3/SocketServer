using System.Net.Sockets;

var port = int.TryParse(Environment.GetEnvironmentVariable("PORT"), out var p) ? p : 1984;
// Слушаем все сетевые интерфейсы (IPv4 и IPv6), чтобы принимать TCP извне, не только с localhost.
var listener = TcpListener.Create(port);
listener.Start();

Console.Error.WriteLine($"TCP *:{port} — все интерфейсы (IPv4/IPv6), входящие байты → stdout");

while (true)
{
    var client = await listener.AcceptTcpClientAsync();
    _ = HandleClientAsync(client);
}

static async Task HandleClientAsync(TcpClient client)
{
    var remote = client.Client.RemoteEndPoint?.ToString() ?? "?";
    await Console.Error.WriteLineAsync($"[connect] {remote}");

    try
    {
        await using var stream = client.GetStream();
        var stdout = Console.OpenStandardOutput();
        var buffer = new byte[8192];
        int n;
        while ((n = await stream.ReadAsync(buffer)) > 0)
            await stdout.WriteAsync(buffer.AsMemory(0, n));
    }
    catch (IOException ex)
    {
        await Console.Error.WriteLineAsync($"[socket error] {remote}: {ex.Message}");
    }
    catch (ObjectDisposedException)
    {
        // клиент закрыл соединение
    }
    finally
    {
        client.Dispose();
        await Console.Error.WriteLineAsync($"[disconnect] {remote}");
    }
}
