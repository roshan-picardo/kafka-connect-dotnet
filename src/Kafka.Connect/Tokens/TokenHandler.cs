using Kafka.Connect.Plugin.Tokens;

namespace Kafka.Connect.Tokens
{
    public class TokenHandler : ITokenHandler
    {
        public void NoOp()
        {
            // this method is majorly used in unit tests
            // as a mock interface to trigger token cancellation
            // eg: usage _tokenHandler.When(x => x.DoNothing()).Do(_ => { if(++counter > 5)  cts.Cancel(); });
        }
    }
}