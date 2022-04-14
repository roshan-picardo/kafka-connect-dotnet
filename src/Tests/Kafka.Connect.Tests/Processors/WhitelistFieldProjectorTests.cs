using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Processors;
using Xunit;

namespace Kafka.Connect.Tests.Processors
{
    public class WhitelistFieldProjectorTests
    {
        private readonly WhitelistFieldProjector _whitelistFieldProjector;

        public WhitelistFieldProjectorTests()
        {
            _whitelistFieldProjector = new WhitelistFieldProjector(null);
        }

        [Fact]
        public async Task Apply_Options()
        {
            var (skip, flattened) = await _whitelistFieldProjector.Apply(
                new Dictionary<string, object>
                {
                    {"key1", "value1"}, 
                    {"key2", "value2"}}, 
                "new[] {\"key.sample.*\", \"key2\", \"key.array[*]*\"}");
            Assert.False(skip);
            Assert.False(flattened.ContainsKey("key1"));
            Assert.True(flattened.ContainsKey("key2"));
        }
        
        [Fact]
        public async Task Apply_Options_Elements()
        {
            var (skip, flattened) = await _whitelistFieldProjector.Apply(
                new Dictionary<string, object>
                {
                    {"key.sample.first", "sample1"}, 
                    {"key.sample.second", "second"}, 
                    {"key.between.remove", "sample1"}, 
                    {"key.between.dont.remove", "data"},
                    {"key.between.keep.this", "second"},
                    {"key.between.keep", "second"},
                    {"key.must.keep", "value2"}}, 
                "new[] {\"key.sample.*\", \"key.*.remove\"}");
            Assert.False(skip);
            Assert.True(flattened.ContainsKey("key.sample.first"));
            Assert.True( flattened.ContainsKey("key.sample.second"));
            Assert.False(flattened.ContainsKey("key.must.keep"));
            Assert.True(flattened.ContainsKey("key.between.remove"));
            Assert.True(flattened.ContainsKey("key.between.dont.remove"));
            Assert.False(flattened.ContainsKey("key.between.keep.this"));
        }
        
        [Fact]
        public async Task Apply_Options_Arrays()
        {
            var (skip, flattened) = await _whitelistFieldProjector.Apply(
                new Dictionary<string, object>
                {
                    {"key.array[0].field", "sample1"},
                    {"key.array[1].field", "second"},
                    {"key.array[1].keep", "second"},
                    
                    {"key.list[0].item.end1", "sample1"},
                    {"key.list[1].item.end2", "data"},
                    {"key.list[1].item1.end2", "data"},
                    
                    {"key.collection[0].item.between1.remove", "second"},
                    {"key.collection[1].item.between2.remove", "second"},
                    {"key.collection[1].item.between2.keep", "second"},
                    {"key.collection[1].item1.between2.keep", "second"},
                    
                    {"key.remove.array[0]", "value2"},
                    {"key.remove.array[1]", "value2"}
                },
                @"new[]
                {
                    ""key.remove.array[*]"", 
                    ""key.array[*].field"", 
                    ""key.list[*].item.*"", 
                    ""key.collection[*].item.*.remove""
                }");
            Assert.False(skip);
            Assert.False(flattened.ContainsKey("key.array[1].keep"));
            Assert.True(flattened.ContainsKey("key.array[0].field"));
            Assert.True(flattened.ContainsKey("key.array[0].field"));
            
            Assert.False(flattened.ContainsKey("key.list[1].item1.end2"));
            Assert.True(flattened.ContainsKey("key.list[0].item.end1"));
            Assert.True(flattened.ContainsKey("key.list[1].item.end2"));
            
            Assert.False(flattened.ContainsKey("key.collection[1].item.between2.keep"));
            Assert.False(flattened.ContainsKey("key.collection[1].item1.between2.keep"));
            Assert.True(flattened.ContainsKey("key.collection[0].item.between1.remove"));
            
            Assert.True(flattened.ContainsKey("key.remove.array[0]"));
            Assert.True(flattened.ContainsKey("key.remove.array[0]"));
        }
    }
}