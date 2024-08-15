using Kafka.Connect.FunctionalTests.Targets.Mongodb;

namespace Kafka.Connect.FunctionalTests.Targets;

public class TargetHelperProvider
{
    private readonly IDictionary<TargetType, ITargetHelper> _helpers;

    public TargetHelperProvider(InitConfig settings)
    {
        _helpers = new Dictionary<TargetType, ITargetHelper>() { { TargetType.Mongodb, new MongodbHelper(settings.Mongodb) } };
    }

    public ITargetHelper GetHelper(TargetType type)
    {
        return _helpers[type];
    }
}