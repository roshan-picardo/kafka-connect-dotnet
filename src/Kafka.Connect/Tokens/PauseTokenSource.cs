using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Timer = System.Timers.Timer;

namespace Kafka.Connect.Tokens;

public class PauseTokenSource
{
    private volatile TaskCompletionSource<bool> _paused;
    internal readonly Task CompletedTask = Task.FromResult(true);
    private Action _onPaused;
    private Action _onResumed;
    private readonly List<CancellationTokenSource> _cancellationTokens = [];
    private readonly Timer _timer = new();

    public PauseTokenSource()
    {
        IsPaused = false;
        _timer.Elapsed += (_, _) => Resume();
    }

    public static PauseTokenSource New(bool paused = false) => new() {IsPaused = paused};
    public bool IsPaused
    {
        get => _paused != null;
        private set
        {
            if (value)
            {
                Interlocked.CompareExchange(
                    ref _paused, new TaskCompletionSource<bool>(), null);
                CancelAll();
                _onPaused?.Invoke();
            }
            else
            {
                Task.Run(() =>
                {
                    while (true)
                    {
                        var tcs = _paused;
                        if (tcs == null) return;
                        if (Interlocked.CompareExchange(ref _paused, null, tcs) != tcs) continue;
                        tcs.SetResult(true);
                        break;
                    }
                });
                _onResumed?.Invoke();
            }
        }
    }

    private PauseToken Token => new(this);

    public PauseTokenSource ConfigureOnPaused(Action onPaused)
    {
        _onPaused = onPaused;
        return this;
    }

    public PauseTokenSource ConfigureOnResumed(Action onResumed)
    {
        _onResumed = onResumed;
        return this;
    }

    internal Task WaitWhilePausedInternal() 
    { 
        var tcs = _paused; 
        return tcs?.Task ?? CompletedTask; 
    }

    public Task WaitWhilePaused(CancellationToken token) => Token.WaitWhilePaused(token);

    public async Task WaitUntilTimeout(int timeoutInMs, CancellationToken token)
    {
        if (timeoutInMs == 0)
        {
            Resume();
            _timer.Enabled = false;
            return;
        }

        await Token.WaitWhilePaused(token);
        _timer.Interval = timeoutInMs;
        _timer.Enabled = true;
        Pause();
    }

    internal void Pause() => Toggle(true);

    internal void Resume() => Toggle(false);

    private void Toggle(bool state)
    {
        IsPaused = state switch
        {
            true when !IsPaused => true, 
            false when IsPaused => false, 
            _ => IsPaused
        };
    }

    public void AddLinkedTokenSource(CancellationTokenSource cts) => _cancellationTokens.Add(cts);

    private void CancelAll()
    {
        foreach (var cancellationToken in _cancellationTokens)
        {
            cancellationToken.Cancel();
        }
        _cancellationTokens.Clear();
    }
}
