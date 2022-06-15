using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Models;
using Kafka.Connect.Processors;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace Kafka.Connect.Tests.Processors
{
    public class BlacklistFieldProjectorTests
    {
        private readonly IOptions<List<ConnectorConfig<IList<string>>>> _options;
        private readonly BlacklistFieldProjector _blacklistFieldProjector;

        public BlacklistFieldProjectorTests()
        {
            _options = Substitute.For<IOptions<List<ConnectorConfig<IList<string>>>>>();
            var shared = Substitute.For<IOptions<ConnectorConfig<IList<string>>>>();
            _blacklistFieldProjector = new BlacklistFieldProjector(_options, shared);
        }

        [Theory]
        [MemberData(nameof(ApplyTests))]
        public async Task Apply_Tests(IDictionary<string, object> data, IList<ConnectorConfig<IList<string>>> options, string[] exists, string[] doesntExists)
        {
            _options.Value.Returns(options);
            var (skip, flattened) = await _blacklistFieldProjector.Apply(data, "connector-name");
            Assert.False(skip);
            Assert.All(exists, key => Assert.True(flattened.ContainsKey(key)));
            Assert.All(doesntExists, key => Assert.False(flattened.ContainsKey(key)));
        }

        public static IEnumerable<object[]> ApplyTests
        {
            get
            {
                yield return new object[] // wrong connector-name
                {
                    new Dictionary<string, object>()
                        {{"key.simple.keep", ""}, {"value.simple.keep", ""}, {"none.simple.keep", ""}},
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "wrong-connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                                {new() {Name = "", Settings = new List<string>()}}
                        }
                    },
                    new[] {"key.simple.keep", "value.simple.keep", "none.simple.keep"},
                    Array.Empty<string>()
                };
                yield return new object[] // Wrong processor
                {
                    new Dictionary<string, object>()
                        {{"key.simple.keep", ""}, {"value.simple.keep", ""}, {"value.none.simple.keep", ""}},
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                            {
                                new()
                                {
                                    Name = "Kafka.Connect.Processors.WhitelistFieldProjector",
                                    Settings = new List<string>()
                                }
                            }
                        }
                    },
                    new[] {"key.simple.keep", "value.simple.keep", "value.none.simple.keep"},
                    Array.Empty<string>()
                };
                yield return new object[] // Remove matching key
                {
                    new Dictionary<string, object>()
                    {
                        {"key.simple.keep", ""}, {"key.simple.remove", ""}, {"value.simple.keep", ""},
                        {"value.simple.remove", ""}, {"value.none.simple.keep", ""}, {"value.none.simple.remove", ""}
                    },
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                            {
                                new()
                                {
                                    Name = "Kafka.Connect.Processors.BlacklistFieldProjector",
                                    Settings = new List<string>
                                        {"key.simple.remove", "value.simple.remove", "none.simple.remove"}
                                }
                            }
                        }
                    },
                    new[] {"key.simple.keep", "value.simple.keep", "value.none.simple.keep"},
                    new[] {"key.simple.remove", "value.simple.remove", "value.none.simple.remove"}
                };
                yield return new object[] // Remove * in between 
                {
                    new Dictionary<string, object>()
                    {
                        {"key.simple.one.keep", ""}, {"key.simple.one.remove", ""}, {"key.simple.two.remove", ""},
                        {"value.simple.one.keep", ""}, {"value.simple.one.remove", ""}, {"value.simple.two.remove", ""},
                        {"value.none.simple.one.keep", ""}, {"value.none.simple.one.remove", ""},
                        {"value.none.simple.two.remove", ""}
                    },
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                            {
                                new()
                                {
                                    Name = "Kafka.Connect.Processors.BlacklistFieldProjector",
                                    Settings = new List<string>
                                        {"key.simple.*.remove", "value.simple.*.remove", "none.simple.*.remove"}
                                }
                            }
                        }
                    },
                    new[] {"key.simple.one.keep", "value.simple.one.keep", "value.none.simple.one.keep"},
                    new[]
                    {
                        "key.simple.one.remove", "value.simple.one.remove", "value.none.simple.one.remove",
                        "key.simple.two.remove", "value.simple.two.remove", "value.none.simple.two.remove"
                    }
                };
                yield return new object[] // Remove * at the end 
                {
                    new Dictionary<string, object>()
                    {
                        {"key.simple.keep.one", ""}, {"key.simple.remove.one", ""}, {"key.simple.remove.two", ""},
                        {"value.simple.keep.one", ""}, {"value.simple.remove.one", ""}, {"value.simple.remove.two", ""},
                        {"value.none.simple.keep.one", ""}, {"value.none.simple.remove.one", ""},
                        {"value.none.simple.remove.two", ""}
                    },
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                            {
                                new()
                                {
                                    Name = "Kafka.Connect.Processors.BlacklistFieldProjector",
                                    Settings = new List<string>
                                        {"key.simple.remove.*", "value.simple.remove.*", "none.simple.remove.*"}
                                }
                            }
                        }
                    },
                    new[] {"key.simple.keep.one", "value.simple.keep.one", "value.none.simple.keep.one"},
                    new[]
                    {
                        "key.simple.remove.one", "value.simple.remove.one", "value.none.simple.remove.one",
                        "key.simple.remove.two", "value.simple.remove.two", "value.none.simple.remove.two"
                    }
                };
                yield return new object[] // Remove multiple *'s
                {
                    new Dictionary<string, object>()
                    {
                        {"key.simple.star.keep.one", ""}, {"key.simple.one.remove.one", ""},
                        {"key.simple.two.remove.two", ""},
                        {"value.simple.star.keep.one", ""}, {"value.simple.one.remove.one", ""},
                        {"value.simple.two.remove.two", ""},
                        {"value.none.simple.star.keep.one", ""}, {"value.none.simple.one.remove.one", ""},
                        {"value.none.simple.two.remove.two", ""}
                    },
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                            {
                                new()
                                {
                                    Name = "Kafka.Connect.Processors.BlacklistFieldProjector",
                                    Settings = new List<string>
                                        {"key.simple.*.remove.*", "value.simple.*.remove.*", "none.simple.*.remove.*"}
                                }
                            }
                        }
                    },
                    new[] {"key.simple.star.keep.one", "value.simple.star.keep.one", "value.none.simple.star.keep.one"},
                    new[]
                    {
                        "key.simple.one.remove.one", "value.simple.one.remove.one", "value.none.simple.one.remove.one",
                        "key.simple.two.remove.two", "value.simple.two.remove.two", "value.none.simple.two.remove.two"
                    }
                };
                yield return new object[] // Remove an element in array at the end 
                {
                    new Dictionary<string, object>()
                    {
                        {"key.simple.keep[0]", ""}, {"key.simple.remove[0]", ""}, {"key.simple.remove[1]", ""},
                        {"value.simple.keep[0]", ""}, {"value.simple.remove[0]", ""}, {"value.simple.remove[1]", ""},
                        {"value.none.simple.keep[0]", ""}, {"value.none.simple.remove[0]", ""},
                        {"value.none.simple.remove[1]", ""}
                    },
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                            {
                                new()
                                {
                                    Name = "Kafka.Connect.Processors.BlacklistFieldProjector",
                                    Settings = new List<string>
                                        {"key.simple.remove[1]", "value.simple.remove[0]", "none.simple.remove[1]"}
                                }
                            }
                        }
                    },
                    new[]
                    {
                        "key.simple.keep[0]", "value.simple.keep[0]", "value.none.simple.keep[0]",
                        "key.simple.remove[0]", "value.none.simple.remove[0]", "value.simple.remove[1]"
                    },
                    new[] {"value.simple.remove[0]", "key.simple.remove[1]", "value.none.simple.remove[1]"}
                };
                yield return new object[] // Remove * in array at the end 
                {
                    new Dictionary<string, object>()
                    {
                        {"key.simple.keep[0]", ""}, {"key.simple.remove[0]", ""}, {"key.simple.two.remove[1]", ""},
                        {"value.simple.keep[0]", ""}, {"value.simple.remove[0]", ""}, {"value.simple.remove[1]", ""},
                        {"value.none.simple.keep[0]", ""}, {"value.none.simple.remove[0]", ""},
                        {"value.none.simple.remove[1]", ""}
                    },
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                            {
                                new()
                                {
                                    Name = "Kafka.Connect.Processors.BlacklistFieldProjector",
                                    Settings = new List<string>
                                        {"key.simple.remove[*]", "value.simple.remove[*]", "none.simple.remove[*]"}
                                }
                            }
                        }
                    },
                    new[] {"key.simple.keep[0]", "value.simple.keep[0]", "value.none.simple.keep[0]"},
                    new[]
                    {
                        "key.simple.remove[0]", "value.simple.remove[0]", "value.none.simple.remove[0]",
                        "key.simple.remove[1]", "value.simple.remove[1]", "value.none.simple.remove[1]"
                    }
                };
                yield return new object[] // Remove * in array in between
                {
                    new Dictionary<string, object>()
                    {
                        {"key.simple.keep[0].child", ""}, {"key.simple.remove[0].child", ""},
                        {"key.simple.remove[1].child", ""},
                        {"value.simple.keep[0].child", ""}, {"value.simple.remove[0].child", ""},
                        {"value.simple.remove[1].child", ""},
                        {"value.none.simple.keep[0].child", ""}, {"value.none.simple.remove[0].child", ""},
                        {"value.none.simple.remove[1].child", ""}
                    },
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                            {
                                new()
                                {
                                    Name = "Kafka.Connect.Processors.BlacklistFieldProjector",
                                    Settings = new List<string>
                                    {
                                        "key.simple.remove[*].child", "value.simple.remove[*].child",
                                        "none.simple.remove[*].child"
                                    }
                                }
                            }
                        }
                    },
                    new[] {"key.simple.keep[0].child", "value.simple.keep[0].child", "value.none.simple.keep[0].child"},
                    new[]
                    {
                        "key.simple.remove[0].child", "value.simple.remove[0].child",
                        "value.none.simple.remove[0].child", "key.simple.remove[1].child",
                        "value.simple.remove[1].child", "value.none.simple.remove[1].child"
                    }
                };
                yield return new object[] // Remove an element in array at the end 
                {
                    new Dictionary<string, object>()
                    {
                        {"key.simple.keep[0].keep", ""}, {"key.simple.remove[0].keep", ""},
                        {"key.simple.remove[1].remove", ""}, {"key.simple.remove[2].remove", ""},
                        {"value.simple.keep[0].keep", ""}, {"value.simple.remove[0].keep", ""},
                        {"value.simple.remove[1].remove", ""}, {"value.simple.remove[2].remove", ""},
                        {"value.none.simple.keep[0].keep", ""}, {"value.none.simple.remove[0].keep", ""},
                        {"value.none.simple.remove[1].remove", ""}, {"value.none.simple.remove[2].remove", ""}
                    },
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                            {
                                new()
                                {
                                    Name = "Kafka.Connect.Processors.BlacklistFieldProjector",
                                    Settings = new List<string>
                                    {
                                        "key.simple.remove[*].remove", "value.simple.remove[*].remove",
                                        "none.simple.remove[*].remove"
                                    }
                                }
                            }
                        }
                    },
                    new[]
                    {
                        "key.simple.keep[0].keep", "value.simple.keep[0].keep", "value.none.simple.keep[0].keep",
                        "key.simple.remove[0].keep", "value.none.simple.remove[0].keep", "value.simple.remove[0].keep"
                    },
                    new[]
                    {
                        "value.simple.remove[1].remove", "key.simple.remove[1].remove",
                        "value.none.simple.remove[1].remove", "value.simple.remove[2].remove",
                        "key.simple.remove[2].remove", "value.none.simple.remove[2].remove"
                    }
                };
                yield return new object[] // Remove an element in array at the end 
                {
                    new Dictionary<string, object>()
                    {
                        {"key.simple.keep[0].keep", ""}, {"key.simple.remove[0].keep.this", ""},
                        {"key.simple.remove[1].remove.this", ""}, {"key.simple.remove[2].remove.that", ""},
                        {"value.simple.keep[0].keep", ""}, {"value.simple.remove[0].keep.this", ""},
                        {"value.simple.remove[1].remove.this", ""}, {"value.simple.remove[2].remove.that", ""},
                        {"value.none.simple.keep[0].keep", ""}, {"value.none.simple.remove[0].keep.this", ""},
                        {"value.none.simple.remove[1].remove.this", ""}, {"value.none.simple.remove[2].remove.that", ""}
                    },
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                            {
                                new()
                                {
                                    Name = "Kafka.Connect.Processors.BlacklistFieldProjector",
                                    Settings = new List<string>
                                    {
                                        "key.simple.remove[*].remove.*", "value.simple.remove[*].remove.*",
                                        "none.simple.remove[*].remove.*"
                                    }
                                }
                            }
                        }
                    },
                    new[]
                    {
                        "key.simple.keep[0].keep", "value.simple.keep[0].keep", "value.none.simple.keep[0].keep",
                        "key.simple.remove[0].keep.this", "value.none.simple.remove[0].keep.this", "value.simple.remove[0].keep.this"
                    },
                    new[]
                    {
                        "value.simple.remove[1].remove.this", "key.simple.remove[1].remove.this",
                        "value.none.simple.remove[1].remove.this", "value.simple.remove[2].remove.that",
                        "key.simple.remove[2].remove.that", "value.none.simple.remove[2].remove.that"
                    }
                };
                
                yield return new object[] // Remove an element in array at the end 
                {
                    new Dictionary<string, object>()
                    {
                        {"key.simple.keep[0].keep", ""}, {"key.simple.remove[0].star.this", ""},
                        {"key.simple.remove[1].star.this", ""}, {"key.simple.remove[2].star.that", ""},
                        {"value.simple.keep[0].keep", ""}, {"value.simple.remove[0].star.this", ""},
                        {"value.simple.remove[1].star.this", ""}, {"value.simple.remove[2].star.that", ""},
                        {"value.none.simple.keep[0].keep", ""}, {"value.none.simple.remove[0].star.this", ""},
                        {"value.none.simple.remove[1].star.this", ""}, {"value.none.simple.remove[2].star.that", ""}
                    },
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                            {
                                new()
                                {
                                    Name = "Kafka.Connect.Processors.BlacklistFieldProjector",
                                    Settings = new List<string>
                                    {
                                        "key.simple.remove[*].*.this", "value.simple.remove[*].*.this",
                                        "none.simple.remove[*].*.this"
                                    }
                                }
                            }
                        }
                    },
                    new[]
                    {
                        "key.simple.keep[0].keep", "value.simple.keep[0].keep", "value.none.simple.keep[0].keep",
                        "key.simple.remove[2].star.that", "value.none.simple.remove[2].star.that", "value.simple.remove[2].star.that"
                    },
                    new[]
                    {
                        "value.simple.remove[1].star.this", "key.simple.remove[1].star.this",
                        "value.none.simple.remove[1].star.this", "value.simple.remove[0].star.this",
                        "key.simple.remove[0].star.this", "value.none.simple.remove[0].star.this"
                    }
                };
                
                yield return new object[] // Remove an element in array at the end 
                {
                    new Dictionary<string, object>()
                    {
                        {"key.simple[0].keep[0].keep", ""}, {"key.simple[0].remove[0].star.this", ""},
                        {"key.simple[0].remove[1].star.this", ""}, {"key.simple[0].remove[2].star.that", ""},
                        {"value.simple[0].keep[0].keep", ""}, {"value.simple[0].remove[0].star.this", ""},
                        {"value.simple[0].remove[1].star.this", ""}, {"value.simple[0].remove[2].star.that", ""},
                        {"value.none.simple[0].keep[0].keep", ""}, {"value.none.simple[0].remove[0].star.this", ""},
                        {"value.none.simple[0].remove[1].star.this", ""}, {"value.none.simple[0].remove[2].star.that", ""}
                    },
                    new List<ConnectorConfig<IList<string>>>
                    {
                        new()
                        {
                            Name = "connector-name",
                            Processors = new List<ProcessorConfig<IList<string>>>()
                            {
                                new()
                                {
                                    Name = "Kafka.Connect.Processors.BlacklistFieldProjector",
                                    Settings = new List<string>
                                    {
                                        "key.simple[*].remove[*].*.this", "value.simple[*].remove[*].*.this",
                                        "none.simple[*].remove[*].*.this"
                                    }
                                }
                            }
                        }
                    },
                    new[]
                    {
                        "key.simple[0].keep[0].keep", "value.simple[0].keep[0].keep", "value.none.simple[0].keep[0].keep",
                        "key.simple[0].remove[2].star.that", "value.none.simple[0].remove[2].star.that", "value.simple[0].remove[2].star.that"
                    },
                    new[]
                    {
                        "value.simple[0].remove[1].star.this", "key.simple[0].remove[1].star.this",
                        "value.none.simple[0].remove[1].star.this", "value.simple[0].remove[0].star.this",
                        "key.simple[0].remove[0].star.this", "value.none.simple[0].remove[0].star.this"
                    }
                };
            }
        }
    }
}