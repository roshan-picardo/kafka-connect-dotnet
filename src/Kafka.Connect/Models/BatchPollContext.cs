using System.Diagnostics;
using System.Threading;

namespace Kafka.Connect.Models
{
    public class BatchPollContext
    {
        private Stopwatch _timer;
        public Stopwatch Timer
        {
            get => _timer ??= new Stopwatch();
            set => _timer = value;
        }
        public CancellationToken Token { get; init; }
        public int Iteration { get; set; }

        public void Reset(int iteration)
        {
            _timer = Stopwatch.StartNew();
            Iteration = iteration;
        }

        public void StartTiming()
        {
            _timer ??= Stopwatch.StartNew();
            if (!_timer.IsRunning)
            {
                _timer.Start();
            }
        }
    }
}