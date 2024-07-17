using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Connect.Tokens;

public readonly struct PauseToken
{
    private readonly PauseTokenSource _pts;

    internal PauseToken(PauseTokenSource pts)
    {
        _pts = pts;
    }

    private bool IsPaused => _pts is {IsPaused: true};

    internal Task WaitWhilePaused(CancellationToken token)
    {
        var pts = _pts;
        token.Register(() =>
        {
            pts.Resume();
        });
        return IsPaused ?  _pts.WaitWhilePausedInternal() :  _pts.CompletedTask;
    }
}
