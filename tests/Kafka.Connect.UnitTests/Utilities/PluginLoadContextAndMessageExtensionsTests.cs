using System.Collections.Generic;
using System.Reflection;
using Confluent.Kafka;
using Kafka.Connect.Utilities;
using Xunit;

namespace UnitTests.Kafka.Connect.Utilities;

public class PluginLoadContextAndMessageExtensionsTests
{
    [Fact]
    public void ToMessageHeaders_WhenDictionaryNull_ReturnsEmptyHeaders()
    {
        var headers = ((IDictionary<string, byte[]>)null).ToMessageHeaders();

        Assert.Empty(headers);
    }

    [Fact]
    public void ToMessageHeaders_WhenDictionaryHasValues_MapsAllEntries()
    {
        IDictionary<string, byte[]> values = new Dictionary<string, byte[]>
        {
            ["a"] = new byte[] { 1 },
            ["b"] = new byte[] { 2, 3 }
        };

        var headers = values.ToMessageHeaders();

        Assert.Equal(2, headers.Count);
        Assert.Equal(new byte[] { 1 }, headers.GetLastBytes("a"));
        Assert.Equal(new byte[] { 2, 3 }, headers.GetLastBytes("b"));
    }

    [Fact]
    public void PluginLoadContext_WhenLoadToDefaultTrue_ReturnsNullFromLoad()
    {
        var context = new PluginLoadContext(typeof(PluginLoadContext).Assembly.Location, loadToDefault: true);

        var result = InvokeLoad(context, new AssemblyName("System.Private.CoreLib"));

        Assert.Null(result);
    }

    [Fact]
    public void PluginLoadContext_WhenAssemblyCannotBeResolved_ReturnsNull()
    {
        var context = new PluginLoadContext(typeof(PluginLoadContext).Assembly.Location);

        var result = InvokeLoad(context, new AssemblyName("Definitely.Not.Real.Assembly"));

        Assert.Null(result);
    }

    private static Assembly InvokeLoad(PluginLoadContext context, AssemblyName assemblyName)
    {
        var method = typeof(PluginLoadContext).GetMethod("Load", BindingFlags.Instance | BindingFlags.NonPublic);
        return method?.Invoke(context, new object[] { assemblyName }) as Assembly;
    }
}
