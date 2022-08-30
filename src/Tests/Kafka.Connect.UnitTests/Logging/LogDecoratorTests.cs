using Kafka.Connect.Logging;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.UnitTests.Logging
{
    public class LogDecoratorTests
    {
        [Fact]
        public void Build_ReturnsDecorated()
        {
            var logDecorator = new LogDecorator();
            var actual = logDecorator.Build<IMethodInfoTester>(new MethodInfoTester(), Substitute.For<MockLogger>());
            Assert.NotNull(actual);
        }
    }
}