using System.Threading;
using System.Threading.Tasks;
using Kafka.Connect.Logging;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;


namespace Kafka.Connect.Tests.Logging
{
    public class MethodInfoExtensionsTests
    {
        private readonly ILogger _logger;

        public MethodInfoExtensionsTests()
        {
            _logger = Substitute.For<MockLogger>();
        }
        
        [Fact]
        public void GetImplementationMethodInfo_Tests()
        {
            var mit = new MethodInfoTester();
            var actual = typeof(IMethodInfoTester).GetMethod("GetString").GetImplementationMethodInfo(mit);
            
            Assert.NotNull(actual);
            Assert.Equal("GetString", actual.Name);
        }

        [Fact]
        public void Invoke_NonAsyncMethodTests()
        {
            var mit = new MethodInfoTester();
            var actual = typeof(IMethodInfoTester).GetMethod("GetString")
                .Invoke(mit, new object[] {10}, _logger, "", null);

            Assert.IsType<string>(actual);
            Assert.Equal(mit.GetString(10), actual);
        }
        
        [Fact]
        public void Invoke_AsyncMethodSuccessTests()
        {
            var mit = new MethodInfoTester();
            var actual = typeof(IMethodInfoTester).GetMethod("GetStringAsync")
                .Invoke(mit, new object[] {10}, _logger, "", null);
            
            Assert.IsType<Task<string>>(actual);
            var task = (Task<string>)actual;
            Assert.Equal(TaskStatus.RanToCompletion, task.Status);
            Assert.Equal(mit.GetString(10), task.Result);
        }
        
        [Fact]
        public void Invoke_AsyncTaskCanceledTests()
        {
            var token = new CancellationToken(true);
            var mit = new MethodInfoTester();
            var actual = typeof(IMethodInfoTester).GetMethod("GetStringCancelled")
                .Invoke(mit, new object[] {10, token}, _logger, "", null);
            
            Assert.IsType<Task>(actual);
            Assert.Equal(TaskStatus.Canceled, ((Task) actual).Status);
        }
        
        [Fact]
        public void Invoke_AsyncTaskFaultedTests()
        {
            var mit = new MethodInfoTester();
            var actual = typeof(IMethodInfoTester).GetMethod("GetStringFaulted")
                .Invoke(mit, new object[] {10}, _logger, "", null);
            
            Assert.IsType<Task>(actual);
            Assert.Equal(TaskStatus.Faulted, ((Task) actual).Status);
        }
        
        
    }
}