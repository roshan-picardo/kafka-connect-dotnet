using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Processors;
using Xunit;

namespace Kafka.Connect.Tests.Processors
{
    public class DateTimeTypeOverriderTests
    {
        private readonly DateTimeTypeOverrider _dateTimeTypeOverrider;

        public DateTimeTypeOverriderTests()
        {
            _dateTimeTypeOverrider = new DateTimeTypeOverrider(null);
        }

        [Fact]
        public async Task Apply_WhenKey_InOptions()
        {
            var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
                new Dictionary<string, object>
                {{"key1", "2020-12-10"}, {"key2", "InValid Date"}}, 
                "new[] { \"key1\"}");
            Assert.False(skip);
            Assert.IsType<DateTime>(flattened["key1"]);
            Assert.IsType<string>(flattened["key2"]);
        }
        
        [Fact]
        public async Task Apply_WhenKey_InOptions_WildCard_Elements()
        {
            var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
                new Dictionary<string, object>
                    {{"key.sample1", "2020-12-10"}, {"key.sample2", "2020-12-10"}}, 
                @"new[] { ""key.*""}, 
                null");
            Assert.False(skip);
            Assert.IsType<DateTime>(flattened["key.sample1"]);
            Assert.IsType<DateTime>(flattened["key.sample2"]);
        }
        
        [Fact]
        public async Task Apply_WhenKey_InOptions_WildCard_Arrays()
        {
            var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
                new Dictionary<string, object>
                    {{"key.array[0]", "2020-12-10"}, {"key.array[1]", "2020-12-15"}}, 
                @"new[] { ""key.array[*]""}, 
                null");
            Assert.False(skip);
            Assert.IsType<DateTime>(flattened["key.array[0]"]);
            Assert.IsType<DateTime>(flattened["key.array[1]"]);
        }
        
        [Fact]
        public async Task Apply_WhenKey_InOptions_WildCard_ArraysAndElements()
        {
            var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
                new Dictionary<string, object>
                    {{"key.array[0].item.first", "2020-12-10"}, {"key.array[1].item.second", "2020-12-13"}}, 
                @"new[] { ""key.array[*].item.*""}, 
                null");
            Assert.False(skip);
            Assert.IsType<DateTime>(flattened["key.array[0].item.first"]);
            Assert.IsType<DateTime>(flattened["key.array[1].item.second"]);
        }
        
        [Fact]
        public async Task Apply_WhenKey_InMaps_WithFormat()
        {
            var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
                new Dictionary<string, object>
                {{"key1", "12-2012-10"}, {"key2", "10-12-2020"}},
                 @"new Dictionary<string, string>()
                {
                    {""key1"", ""MM-yyyy-dd""},
                    {""Key2"", ""dd/MM/yyyy""}
                }");
            Assert.False(skip);
            Assert.IsType<DateTime>(flattened["key1"]);
            Assert.IsType<string>(flattened["key2"]);
        }
        
        [Fact]
        public async Task Apply_WhenKey_InMaps_WildCard_Elements_Format()
        {
            var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
                new Dictionary<string, object>
                    {{"key.sample1", "12-2020-10"}, {"key.sample2", "12-2020-10"}}, 
                @"maps: new Dictionary<string, string>()
                {
                    {""key*"", ""MM-yyyy-dd""}
                }");
            Assert.False(skip);
            Assert.IsType<DateTime>(flattened["key.sample1"]);
            Assert.IsType<DateTime>(flattened["key.sample2"]);
        }
        
        [Fact]
        public async Task Apply_WhenKey_InMaps_WildCard_Arrays_Format()
        {
            var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
                new Dictionary<string, object>
                    {{"key.array[0]", "12-2020-10"}, {"key.array[1]", "12-2020-10"}}, 
                @"maps: new Dictionary<string, string>()
                {
                    {""key.array[*]"", ""MM-yyyy-dd""}
                }");
            Assert.False(skip);
            Assert.IsType<DateTime>(flattened["key.array[0]"]);
            Assert.IsType<DateTime>(flattened["key.array[1]"]);
        }
        
        [Fact]
        public async Task Apply_WhenKey_InMaps_WildCard_ArraysAndElements_Format()
        {
            var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
                new Dictionary<string, object>
                    {{"key.array[0].item.first", "12-2020-10"}, {"key.array[1].item.second", "12-2020-10"}}, 
                @"maps: new Dictionary<string, string>()
                {
                    {""key.array[*].item.*"", ""MM-yyyy-dd""}
                }");
            Assert.False(skip);
            Assert.IsType<DateTime>(flattened["key.array[0].item.first"]);
            Assert.IsType<DateTime>(flattened["key.array[1].item.second"]);
        }
        
        [Fact]
        public async Task Apply_Ignore_WhenValue_IsNull()
        {
            var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
                new Dictionary<string, object>
                    {{"key1", null}},
                @"maps: new Dictionary<string, string>()
                {
                    {""key1"", ""MM-yyyy-dd""}
                }");
            Assert.False(skip);
            Assert.Null(flattened["key1"]);
        }
        
        [Fact]
        public async Task Apply_Ignore_WhenValue_IsNotString()
        {
            var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
                new Dictionary<string, object>
                    {{"key1", 500}},
                @"maps: new Dictionary<string, string>()
                {
                    {""key1"", ""MM-yyyy-dd""}
                }");
            Assert.False(skip);
            Assert.IsType<int>(flattened["key1"]);
        }
        
        [Fact]
        public async Task Apply_WhenKey_InMapsAndOptions_MergeKeys()
        {
            var (skip, flattened) = await _dateTimeTypeOverrider.Apply(
                new Dictionary<string, object>
                    {{"key1", "12-2012-10"}, {"key2", "2020-12-25"}},
                @"maps: new Dictionary<string, string>()
                {
                    {""key1"", ""MM-yyyy-dd""}
                },
                options:new List<string>() { ""key2""}");
            Assert.False(skip);
            Assert.IsType<DateTime>(flattened["key1"]);
            Assert.IsType<DateTime>(flattened["key2"]);
        }
        
    }
}