using System;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace TriggerWatch;

/// <summary>
/// 検知されたフォルダの情報
/// </summary>
public sealed record TriggeredFolder(
    string FolderPath,
    string FilePath,
    DateTimeOffset Timestamp
);

/// <summary>
/// 特定ファイルの生成を合図に、準備完了したフォルダを安全に通知する監視クラス
/// </summary>
public sealed class FolderWatcher : IDisposable
{
    private readonly string _path;
    private readonly string _filter;
    private readonly TimeSpan _lockTimeout;

    private FileSystemWatcher? _watcher;
    private Channel<string>? _channel;
    private CancellationTokenSource? _cts;
    private Task? _workerTask;

    /// <summary>
    /// 現在監視が動いているかどうか
    /// </summary>
    public bool IsRunning => _watcher?.EnableRaisingEvents ?? false;

    public FolderWatcher(string path, string filter, TimeSpan? lockTimeout = null)
    {
        _path = path;
        _filter = filter;
        _lockTimeout = lockTimeout ?? TimeSpan.FromSeconds(5);
    }

    /// <summary>
    /// 監視を開始する（非同期ハンドラ用）
    /// 渡した関数の完了を待ってから次の検知を通知するため、処理の重複が起きません。
    /// </summary>
    public void Start(Func<TriggeredFolder, Task> onDetected)
    {
        if (IsRunning) return;

        _cts = new CancellationTokenSource();
        _channel = Channel.CreateUnbounded<string>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = true
        });

        _watcher = new FileSystemWatcher(_path)
        {
            IncludeSubdirectories = true,
            Filter = _filter,
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite,
            InternalBufferSize = 64 * 1024
        };

        _watcher.Created += (_, e) => _channel.Writer.TryWrite(e.FullPath);
        _watcher.Renamed += (_, e) => _channel.Writer.TryWrite(e.FullPath);
        _watcher.EnableRaisingEvents = true;

        _workerTask = Task.Run(() => ProcessQueueAsync(_channel.Reader, onDetected, _cts.Token));
    }

    /// <summary>
    /// 監視を開始する（同期ハンドラ用）
    /// </summary>
    public void Start(Action<TriggeredFolder> onDetected)
    {
        Start(folder =>
        {
            onDetected(folder);
            return Task.CompletedTask;
        });
    }

    /// <summary>
    /// 監視を停止する
    /// </summary>
    public void Stop()
    {
        if (_watcher != null)
        {
            _watcher.EnableRaisingEvents = false;
            _watcher.Dispose();
            _watcher = null;
        }

        _cts?.Cancel();
        _channel?.Writer.TryComplete();
    }

    private async Task ProcessQueueAsync(
        ChannelReader<string> reader, 
        Func<TriggeredFolder, Task> handler, 
        CancellationToken ct)
    {
        try
        {
            while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                while (reader.TryRead(out var filePath))
                {
                    // ファイルが書き込み終了（排他ロック解除）するのを待つ
                    if (!await TryWaitUntilUnlockedAsync(filePath, _lockTimeout, ct).ConfigureAwait(false))
                    {
                        continue;
                    }

                    string folderPath = Path.GetDirectoryName(filePath)!;
                    var folder = new TriggeredFolder(folderPath, filePath, DateTimeOffset.UtcNow);

                    // コールバックの完了を待機（これでメイン側の処理と絶対に競合しない）
                    await handler(folder).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    private static async Task<bool> TryWaitUntilUnlockedAsync(string filePath, TimeSpan timeout, CancellationToken ct)
    {
        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

        while (!linkedCts.Token.IsCancellationRequested)
        {
            try
            {
                using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.None);
                return true;
            }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }

            try
            {
                await Task.Delay(100, linkedCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        return false;
    }

    public void Dispose() => Stop();
}
