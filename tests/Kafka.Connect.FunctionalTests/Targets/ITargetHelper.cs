namespace Kafka.Connect.FunctionalTests.Targets;

public interface ITargetHelper
{
    Task Setup(Sink data);
    Task Cleanup(Sink data);
    Task<(bool, string)> Validate(Sink data);
}