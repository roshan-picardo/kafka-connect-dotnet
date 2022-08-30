using System;
using System.Collections.Generic;
using System.Reflection;
using Kafka.Connect.Logging;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Logging
{
    public class LogDispatchProxyTests : LogDispatchProxy<IMethodInfoTester>
    {
        [Theory]
        [MemberData(nameof(CreateProxyTests))]
        public void Create_ThrowsArgumentExceptionTests(IMethodInfoTester decorated, ILogger logger, string expectedMessage)
        {
            var actual = Assert.Throws<ArgumentNullException>(() => Create(decorated, logger));
            Assert.Equal(expectedMessage, actual.Message);
        }
        
        [Fact]
        public void Create_ReturnsProxy()
        {
            var mit = new MethodInfoTester();
            var logger = Substitute.For<MockLogger>();
            var actual =  Create(mit, logger);
            
            Assert.NotNull(actual);
        }

        [Theory]
        [InlineData("WithoutOperationLog", "WithoutOperationLog")]
        [InlineData("WithOperationLog", "WithOperationLog")]
        public void Invoke_WithOrWithoutOperationLog(string methodToCall, string expected)
        {
            var mit = new MethodInfoTester();
            var methodInfo = typeof(IMethodInfoTester).GetMethod(methodToCall);
            SetParameters(mit, Substitute.For<MockLogger>());
            
            var actual = Invoke(methodInfo, null);
            Assert.Equal(expected, actual);
        }

        [Theory]
        [MemberData(nameof(InvokeExceptionTests))]
        public void Invoke_ThrowsTargetInvocationException(Exception input, Type expected)
        {
            var methodInfo = typeof(IMethodInfoTester).GetMethod("Throws");
            SetParameters(new MethodInfoTester(), Substitute.For<MockLogger>());
           Assert.Throws(expected, () => Invoke(methodInfo, new object[]{ input}));
        }

        public static IEnumerable<object[]> CreateProxyTests
        {
            get
            {
                yield return new object[] {null, null, "Value cannot be null. (Parameter 'decorated')"};
                yield return new object[] {new MethodInfoTester(), null, "Value cannot be null. (Parameter 'logger')"};
            }
        }

        public static IEnumerable<object[]> InvokeExceptionTests
        {
            get
            {
                yield return new object[] {new TargetInvocationException(null), typeof(TargetInvocationException)};
                yield return new object[] {new Exception(), typeof(Exception)};

            }
        }

    }
}