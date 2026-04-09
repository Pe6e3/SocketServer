using System.Globalization;
using System.Net.Sockets;
using System.Text;
using Terminal.Gui;

static string PrettyLogLine(string raw)
{
    var tab = raw.IndexOf('\t');
    if (tab <= 0)
        return raw;
    var iso = raw.AsSpan(0, tab);
    var msg = raw[(tab + 1)..];
    if (DateTime.TryParse(iso, null, DateTimeStyles.RoundtripKind, out var utc))
        return $"{utc.ToLocalTime():HH:mm:ss.fff}  {msg}";
    return raw;
}

var host = args.Length > 0 ? args[0] : "127.0.0.1";
var dataPort = args.Length > 1 && int.TryParse(args[1], out var dp) ? dp : 1984;
var logPort = args.Length > 2 && int.TryParse(args[2], out var lp) ? lp : dataPort + 1;
var plainMode = args.Any(a => a is "--plain" or "-p") || Environment.GetEnvironmentVariable("SOCKET_CONSOLE_PLAIN") == "1";

using var dataClient = new TcpClient();
using var logClient = new TcpClient();
try
{
    await Task.WhenAll(dataClient.ConnectAsync(host, dataPort), logClient.ConnectAsync(host, logPort))
        .ConfigureAwait(false);
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Не удалось подключиться к {host}:{dataPort} / {host}:{logPort}");
    Console.Error.WriteLine(ex.Message);
    return 1;
}

var dataStream = dataClient.GetStream();
await using var logStream = logClient.GetStream();
using var logReader = new StreamReader(logStream, Encoding.UTF8, leaveOpen: true);

if (plainMode)
{
    while (true)
    {
        var line = await logReader.ReadLineAsync().ConfigureAwait(false);
        if (line == null)
            break;
        Console.WriteLine(PrettyLogLine(line));
    }
    return 0;
}

var logLines = new List<string>(512);
const int maxLogLines = 4000;
var logLinesLock = new object();

Application.Init();

try
{
    var top = Application.Top;

    var logView = new TextView
    {
        X = 0,
        Y = 0,
        Width = Dim.Fill(),
        Height = Dim.Fill() - 1,
        ReadOnly = true,
        AllowsTab = false,
        WordWrap = true,
        ColorScheme = new ColorScheme
        {
            Normal = Application.Driver.MakeAttribute(Color.Gray, Color.Black),
            Focus = Application.Driver.MakeAttribute(Color.Black, Color.DarkGray),
        },
    };

    var input = new TextField("")
    {
        X = 0,
        Y = 0,
        Width = Dim.Fill(),
    };

    var statusFrame = new FrameView("Ввод (UTF-8), Enter — отправить строку на сервер")
    {
        X = 0,
        Y = Pos.Bottom(logView),
        Width = Dim.Fill(),
        Height = 3,
    };

    statusFrame.Add(input);

    var win = new Window($"TCP  {host}   данные :{dataPort}   логи :{logPort}")
    {
        X = 0,
        Y = 1,
        Width = Dim.Fill(),
        Height = Dim.Fill() - 1,
    };

    win.Add(logView);
    win.Add(statusFrame);

    void AppendLog(string line)
    {
        lock (logLinesLock)
        {
            logLines.Add(line);
            if (logLines.Count > maxLogLines)
                logLines.RemoveRange(0, logLines.Count - maxLogLines + 500);
            logView.Text = string.Join(Environment.NewLine, logLines);
            logView.CursorPosition = new Point(0, Math.Max(0, logLines.Count - 1));
        }
    }

    var menu = new MenuBar(new[]
    {
        new MenuBarItem("_Поток", new[]
        {
            new MenuItem("_Очистить лог", "", () =>
            {
                lock (logLinesLock)
                {
                    logLines.Clear();
                    logView.Text = "";
                }
            }),
            new MenuItem("_Выход", "Ctrl+Q", () => Application.RequestStop(), shortcut: Key.Q | Key.CtrlMask),
        }),
    });

    top.Add(menu);
    top.Add(win);

    async Task SendLineAsync()
    {
        var text = input.Text?.ToString() ?? "";
        if (text.Length == 0)
            return;
        var payload = Encoding.UTF8.GetBytes(text + "\n");
        try
        {
            await dataStream.WriteAsync(payload).ConfigureAwait(false);
            await dataStream.FlushAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Application.MainLoop.Invoke(() => AppendLog($"[локально] ошибка отправки: {ex.Message}"));
            return;
        }

        Application.MainLoop.Invoke(() => input.Text = "");
    }

    input.KeyDown += e =>
    {
        if (e.KeyEvent.Key != Key.Enter)
            return;
        e.Handled = true;
        _ = SendLineAsync();
    };

    var logCts = new CancellationTokenSource();
    var readLogs = Task.Run(async () =>
    {
        try
        {
            while (!logCts.Token.IsCancellationRequested)
            {
                var line = await logReader.ReadLineAsync(logCts.Token).ConfigureAwait(false);
                if (line == null)
                    break;
                var pretty = PrettyLogLine(line);
                Application.MainLoop.Invoke(() => AppendLog(pretty));
            }
        }
        catch (OperationCanceledException)
        {
            // нормальное завершение
        }
        catch (Exception ex)
        {
            Application.MainLoop.Invoke(() => AppendLog($"[локально] поток логов: {ex.Message}"));
        }
    }, logCts.Token);

    Application.Run();

    logCts.Cancel();
    try
    {
        await readLogs.ConfigureAwait(false);
    }
    catch
    {
        // игнор
    }
}
finally
{
    Application.Shutdown();
}

return 0;
