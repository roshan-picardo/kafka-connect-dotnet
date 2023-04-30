using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Connect.Tokens
{
    public class PauseTokenSource
    {
        private volatile TaskCompletionSource<bool> _paused;
        internal readonly Task CompletedTask = Task.FromResult(true);
        private Action _onPaused;
        private Action _onResumed;
        private readonly IList<CancellationTokenSource> _cancellationTokens = new List<CancellationTokenSource>();

        public static PauseTokenSource New(bool paused = false) => new() {IsPaused = paused};
        public bool IsPaused
        {
            get => _paused != null;
            set
            {
                if (value)
                {
                    Interlocked.CompareExchange(
                        ref _paused, new TaskCompletionSource<bool>(), null);
                    ResumePayload = null;
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

        public PauseToken Token => new(this);

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

        internal Task WaitWhilePausedAsync() 
        { 
            var tcs = _paused; 
            return (tcs?.Task ?? CompletedTask); 
        }

        internal void Pause() => Toggle(true);

        internal void Resume() => Toggle(false);

        internal void Toggle(bool state)
        {
            IsPaused = state switch
            {
                true when !IsPaused => true, 
                false when IsPaused => false, 
                _ => IsPaused
            };
        }

        public IDictionary<string, string> ResumePayload { get; set; }

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
}