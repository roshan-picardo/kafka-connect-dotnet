using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Kafka.Connect.Converters;
using Kafka.Connect.Plugin.Logging;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace UnitTests.Kafka.Connect.Converters;

public class JsonRecordFlattenerTests
{
    private readonly JsonRecordFlattener _jsonRecordFlattener;

    public JsonRecordFlattenerTests()
    {
        _jsonRecordFlattener = new JsonRecordFlattener(Substitute.For<ILogger<JsonRecordFlattener>>());
    }

    [Fact]
    public void Flatten_Flat_Json()
    {
        var dateTest = new DateTime(2020, 10, 10, 10, 10, 10);
        var token = new JObject()
            {{"string", "value1"}, {"number", 100}, {"datetime", dateTest}};
            
        var expected = new Dictionary<string, object>
        {
            {"string", "value1"}, {"number", 100}, {"datetime", dateTest}
        };

        var actual = _jsonRecordFlattener.Flatten((JToken)token);
        Assert.Equal(expected["string"], actual["string"]);
        Assert.Equal(expected["number"], actual["number"]);
       // Assert.Equal(expected["datetime"], actual["datetime"]);
    }
        
    [Fact]
    public void Flatten_Structured_Json()
    {
        var data = new
        {
            parent = new
            {
                child1 = "firstChild",
                child2 = new
                {
                    grandChild = "grandchild"
                }
            }
        };
            
        var token = JToken.FromObject(data);
            
        var expected = new Dictionary<string, object>
        {
            {"parent.child1", "firstChild"}, {"parent.child2.grandChild", "grandchild"}
        };

        var actual = _jsonRecordFlattener.Flatten(token);
            
        Assert.Equal(expected["parent.child1"], actual["parent.child1"]);
        Assert.Equal(expected["parent.child2.grandChild"], actual["parent.child2.grandChild"]);
    }
        
    [Fact]
    public void Flatten_Arrays_Json()
    {
        var data = new
        {
            parent = new
            {
                children = new []
                {
                    new
                    {
                        name = "me"
                    },
                    new
                    {
                        name = "you"
                    }
                },
                friend = "friend"
            }
        };
            
        var token = JToken.FromObject(data);
            
        var expected = new Dictionary<string, object>
        {
            {"parent.friend", "friend"}, {"parent.children[0].name", "me"}, {"parent.children[1].name", "you"}
        };

        var actual = _jsonRecordFlattener.Flatten(token);
            
        Assert.Equal(expected["parent.friend"], actual["parent.friend"]);
        Assert.Equal(expected["parent.children[0].name"], actual["parent.children[0].name"]);
        Assert.Equal(expected["parent.children[1].name"], actual["parent.children[1].name"]);
    }

    [Fact]
    public void Flatten_EmptyElementAndArray_Json()
    {
        var data = new
        {
            parent = new
            {
                item = new { },
                array = new List<string>()
            }
        };

        var token = JToken.FromObject(data);

        var expected = new Dictionary<string, object>
        {
            {"parent.item", new object()}, {"parent.array", Enumerable.Empty<object>()}
        };

        var actual = _jsonRecordFlattener.Flatten(token);

        Assert.IsType<object>(expected["parent.item"]);
        Assert.IsType<object>(actual["parent.item"]);
    }

    [Fact]
    public void Unflatten_Flat_Structure()
    {
        var dateTest = new DateTime(2020, 10, 10, 10, 10, 10);
        var input = new Dictionary<string, object>
        {
            {"string", "value1"}, {"number", (long)100}, {"datetime", dateTest}
        };
            
        var expected = new JObject()
            {{"string", "value1"}, {"number", 100}, {"datetime", dateTest}};

        var actual = _jsonRecordFlattener.Unflatten(input);
            
        Assert.Equal(expected, actual);
    }
        
    [Fact]
    public void Unflatten_Structured_Json()
    {
        var input = new Dictionary<string, object>
        {
            {"parent.child1", "firstChild"}, {"parent.child2.grandChild", "grandchild"}
        };
            
        var data = new
        {
            parent = new
            {
                child1 = "firstChild",
                child2 = new
                {
                    grandChild = "grandchild"
                }
            }
        };
        var expected = JToken.FromObject(data);


        var actual = _jsonRecordFlattener.Unflatten(input);
            
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Unflatten_Arrays_Json()
    {
        var input = new Dictionary<string, object>
        {
            {"parent.friend", "friend"}, {"parent.children[0].name", "me"}, {"parent.children[1].name", "you"}
        };
        var data = new
        {
            parent = new
            {
                friend = "friend",
                children = new[]
                {
                    new
                    {
                        name = "me"
                    },
                    new
                    {
                        name = "you"
                    }
                }
            }
        };
        var expected = JToken.FromObject(data);

        var actual = _jsonRecordFlattener.Unflatten(input);

        Assert.Equal(expected, actual);
    }
}