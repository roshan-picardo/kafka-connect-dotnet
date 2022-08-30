using System;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Logging;

namespace Kafka.Connect.UnitTests.Logging
{
    public class MethodInfoTester : IMethodInfoTester
    {
        public string GetString(int no)
        {
            return $"No = {no}";
        }

        public string WithoutOperationLog()
        {
            return nameof(WithoutOperationLog);
        }
        
        [OperationLog("OperationLog")]
        public string WithOperationLog()
        {
            return nameof(WithOperationLog);
        }

        public Task<string> GetStringAsync(int no)
        {
            return Task.FromResult($"No = {no}");
        }
        
        public Task GetStringCancelled(int no, CancellationToken token)
        {
            return Task.FromCanceled(token);
        }

        public Task GetStringFaulted(int no)
        {
            return Task.FromException(new Exception("Task Faulted"));
        }
        
        public string Throws(Exception ex)
        {
            throw  ex;
        }
    }

    public interface IMethodInfoTester
    {
         string GetString(int no);
         Task<string> GetStringAsync(int no);
         Task GetStringCancelled(int no, CancellationToken token);
         string WithoutOperationLog();
         string WithOperationLog();
         Task GetStringFaulted(int no);
         string Throws(Exception ex);
    }
}