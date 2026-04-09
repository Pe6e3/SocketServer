using System.Net;
using System.Net.Sockets;
using System.Text;

namespace SocketServer;

internal static class SocketLoggerHttpServer
{
    private const string Html = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Socket Logger</title>
  <style>
    :root { color-scheme: dark; --bg:#0b1020; --panel:#111933; --text:#e5ecff; --muted:#8ea0d5; --accent:#6ea8fe; --ok:#6ef3b8; }
    * { box-sizing:border-box; }
    body { margin:0; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; background:linear-gradient(160deg,#0b1020,#0c1738); color:var(--text); }
    .wrap { max-width:1100px; margin:0 auto; padding:20px; }
    .head { display:flex; gap:12px; align-items:center; justify-content:space-between; margin-bottom:12px; }
    .title { font-size:20px; font-weight:700; }
    .status { color:var(--ok); font-size:13px; }
    .panel { background:rgba(17,25,51,.9); border:1px solid rgba(255,255,255,.08); border-radius:14px; padding:12px; box-shadow:0 15px 40px rgba(0,0,0,.35); }
    .tools { display:flex; gap:10px; margin-bottom:10px; }
    input,button { background:#0a1430; color:var(--text); border:1px solid #2d3f7a; border-radius:10px; padding:8px 10px; font:inherit; }
    input { flex:1; }
    button { cursor:pointer; }
    #log { height:72vh; overflow:auto; white-space:pre-wrap; line-height:1.45; font-size:13px; color:#dbe5ff; }
    .line { border-bottom:1px dashed rgba(255,255,255,.06); padding:2px 0; }
    .muted { color:var(--muted); }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="head">
      <div class="title">Socket Logger</div>
      <div class="status" id="status">connecting...</div>
    </div>
    <div class="panel">
      <div class="tools">
        <input id="filter" placeholder="Фильтр (substring)" />
        <button id="clear">Очистить</button>
      </div>
      <div id="log"></div>
      <div class="muted">SSE поток: <code>/socket-logger/logs</code></div>
    </div>
  </div>
  <script>
    const status = document.getElementById('status');
    const log = document.getElementById('log');
    const filter = document.getElementById('filter');
    const clearBtn = document.getElementById('clear');
    const lines = [];
    let q = '';
    const max = 4000;

    function decodeEscapes(text) {
      return text
        .replace(/\\r\\n/g, '\n')
        .replace(/\\n/g, '\n')
        .replace(/\\r/g, '\n')
        .replace(/\\t/g, '\t');
    }

    function render() {
      const frag = document.createDocumentFragment();
      let shown = 0;
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        if (q && !line.toLowerCase().includes(q)) continue;
        const div = document.createElement('div');
        div.className = 'line';
        div.textContent = decodeEscapes(line);
        frag.appendChild(div);
        shown++;
      }
      log.replaceChildren(frag);
      log.scrollTop = log.scrollHeight;
      status.textContent = `online • lines: ${lines.length} • shown: ${shown}`;
    }

    filter.addEventListener('input', () => { q = filter.value.trim().toLowerCase(); render(); });
    clearBtn.addEventListener('click', () => { lines.length = 0; render(); });

    const es = new EventSource('./logs');
    es.onopen = () => { status.textContent = 'online'; };
    es.onmessage = (evt) => {
      const decoded = decodeEscapes(evt.data);
      lines.push(decoded);
      if (lines.length > max) lines.splice(0, lines.length - max + 200);
      if (!q || decoded.toLowerCase().includes(q)) {
        const div = document.createElement('div');
        div.className = 'line';
        div.textContent = decoded;
        log.appendChild(div);
        log.scrollTop = log.scrollHeight;
      }
    };
    es.onerror = () => { status.textContent = 'reconnecting...'; };
  </script>
</body>
</html>
""";

    public static Task RunAsync(int httpPort, int logPort, CancellationToken cancellationToken)
    {
        var listener = new HttpListener();
        listener.Prefixes.Add($"http://*:{httpPort}/");
        listener.Start();
        return Task.Run(async () =>
        {
            await ServerLogging.BroadcastAsync($"[http] logger ui http://*:{httpPort}/", cancellationToken).ConfigureAwait(false);
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    HttpListenerContext ctx;
                    try
                    {
                        ctx = await listener.GetContextAsync().ConfigureAwait(false);
                    }
                    catch (HttpListenerException)
                    {
                        break;
                    }
                    _ = HandleAsync(ctx, logPort, cancellationToken);
                }
            }
            finally
            {
                listener.Stop();
            }
        }, cancellationToken);
    }

    private static async Task HandleAsync(HttpListenerContext ctx, int logPort, CancellationToken cancellationToken)
    {
        var req = ctx.Request;
        var res = ctx.Response;
        var path = req.Url?.AbsolutePath ?? "/";
        if (path == "/" || path == "/index.html")
        {
            res.StatusCode = 200;
            res.ContentType = "text/html; charset=utf-8";
            var bytes = Encoding.UTF8.GetBytes(Html);
            await res.OutputStream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
            res.Close();
            return;
        }

        if (path == "/logs")
        {
            await ServeSseAsync(res, logPort, cancellationToken).ConfigureAwait(false);
            return;
        }

        res.StatusCode = 404;
        res.Close();
    }

    private static async Task ServeSseAsync(HttpListenerResponse res, int logPort, CancellationToken cancellationToken)
    {
        res.StatusCode = 200;
        res.ContentType = "text/event-stream";
        res.Headers["Cache-Control"] = "no-cache";
        res.Headers["X-Accel-Buffering"] = "no";
        res.SendChunked = true;
        await using var writer = new StreamWriter(res.OutputStream, new UTF8Encoding(false), leaveOpen: true);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, logPort, cancellationToken).ConfigureAwait(false);
        await using var stream = client.GetStream();
        using var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);

        while (!cancellationToken.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (line == null)
                break;
            await writer.WriteAsync("data: ").ConfigureAwait(false);
            await writer.WriteAsync(line.Replace("\r", "").Replace("\n", " ")).ConfigureAwait(false);
            await writer.WriteAsync("\n\n").ConfigureAwait(false);
            await writer.FlushAsync().ConfigureAwait(false);
        }
    }
}
