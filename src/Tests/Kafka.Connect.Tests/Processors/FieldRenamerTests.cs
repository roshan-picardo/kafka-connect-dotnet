using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Processors;
using Xunit;

namespace Kafka.Connect.Tests.Processors
{
    public class FieldRenamerTests
    {
        private readonly FieldRenamer _fieldRenamer;

        public FieldRenamerTests()
        {
            _fieldRenamer = new FieldRenamer(null);
        }

        [Fact]
        public async Task Apply_Maps()
        {
            var (skip, flattened) = await _fieldRenamer.Apply(
                new Dictionary<string, object>
                {
                    {"flat", "value1"}, 
                    {"nested", "value1"},
                    {"old.nested", "value"},
                    {"key2", "value2"}}, 
                @"maps: new Dictionary<string, string>()
                {
                    {""flat"", ""new""},
                    {""nested"", ""new.nested""},
                    {""old.nested"", ""makeFlat""},
                }");
            Assert.False(skip);
            Assert.True(flattened.ContainsKey("value.new"));
            Assert.False(flattened.ContainsKey("value.flat"));
            
            Assert.False(flattened.ContainsKey("value.nested"));
            Assert.True(flattened.ContainsKey("value.new.nested"));
            
            Assert.False(flattened.ContainsKey("value.old.nested"));
            Assert.True(flattened.ContainsKey("value.makeFlat"));
            
            Assert.True(flattened.ContainsKey("key2"));
        }
        
        [Fact]
        public async Task Apply_Maps_WildCard_Arrays()
        {
            var (skip, flattened) = await _fieldRenamer.Apply(
                new Dictionary<string, object>
                {
                    {"key.array[0].first", "sample1"}, 
                    {"key.array[1].first", "second"}}, 
                @"maps: new Dictionary<string, string>()
                {
                    {""key.array[*].first"", ""new.array[*].renamed""}
                }");
            Assert.False(skip);
            Assert.False(flattened.ContainsKey("key.array[0].first"));
            Assert.False(flattened.ContainsKey("key.array[1].first"));
            Assert.True(flattened.ContainsKey("value.new.array[0].renamed"));
            Assert.True(flattened.ContainsKey("value.new.array[1].renamed"));
        }
        
        [Fact]
        public async Task Apply_Maps_WildCard_Elements()
        {
            var (skip, flattened) = await _fieldRenamer.Apply(
                new Dictionary<string, object>
                {
                    {"end.wildcard.one", "wildcard"},
                    {"end.wildcard.two", "wildcard"},
                    
                    {"wildcard.one.start", "wildcard"},
                    {"wildcard.two.start", "wildcard"},
                    
                    {"start.one.end", "wildcard"},
                    {"start.two.end", "wildcard"},
                    }, 
                @"maps: new Dictionary<string, string>()
                {
                    {""end.*"", ""renamed.*""},
                    {""*.start"", ""*.renamed""},
                    {""start.*.end"", ""renamed.*.renamed""}
                }");
            Assert.False(skip);
            Assert.False(flattened.ContainsKey("end.wildcard.one"));
            Assert.False(flattened.ContainsKey("end.wildcard.two"));
            Assert.True(flattened.ContainsKey("value.renamed.wildcard.one"));
            Assert.True(flattened.ContainsKey("value.renamed.wildcard.two"));
            
            Assert.False(flattened.ContainsKey("wildcard.one.start"));
            Assert.False(flattened.ContainsKey("wildcard.one.start"));
            Assert.True(flattened.ContainsKey("value.wildcard.one.renamed"));
            Assert.True(flattened.ContainsKey("value.wildcard.one.renamed"));
            
            Assert.False(flattened.ContainsKey("start.one.end"));
            Assert.False(flattened.ContainsKey("start.two.end"));
            Assert.True(flattened.ContainsKey("value.renamed.one.renamed"));
            Assert.True(flattened.ContainsKey("value.renamed.two.renamed"));
        }
        
        [Fact]
        public async Task Apply_Ignore_WhenValue_IsNull()
        {
            var (skip, flattened) = await _fieldRenamer.Apply(
                new Dictionary<string, object>
                    {{"key1", null}},
                @"maps: new Dictionary<string, string>()
                {
                    {""key1"", ""newKey""}
                }");
            Assert.False(skip);
            Assert.Null(flattened["key1"]);
        }
    }
}