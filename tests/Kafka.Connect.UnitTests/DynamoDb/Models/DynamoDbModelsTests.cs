using Amazon.DynamoDBv2.Model;
using Kafka.Connect.DynamoDb.Models;
using Xunit;

namespace UnitTests.Kafka.Connect.DynamoDb.Models;

public class StreamModelTests
{
    [Fact]
    public void StreamModel_CreateWithProperties_AllPropertiesSet()
    {
        // Arrange & Act
        var streamModel = new StreamModel
        {
            Operation = "GET_RECORDS",
            TableName = "my-table",
            StreamArn = "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable/stream/2015-05-11T21:21:33.291",
            ShardId = "shardId-000000000000",
            ShardIterator = "arn:aws:dynamodb:us-west-2:123456789012:table/MyTable/stream/2015-05-11T21:21:33.291|1|AAH/BXj",
            ShardIteratorType = "TRIM_HORIZON",
            SequenceNumber = "12345678",
            Request = new GetRecordsRequest { Limit = 100 }
        };

        // Assert
        Assert.Equal("GET_RECORDS", streamModel.Operation);
        Assert.Equal("my-table", streamModel.TableName);
        Assert.Equal("shardId-000000000000", streamModel.ShardId);
        Assert.Equal("TRIM_HORIZON", streamModel.ShardIteratorType);
        Assert.NotNull(streamModel.Request);
        Assert.Equal(100, streamModel.Request.Limit);
    }

    [Fact]
    public void StreamModel_WithNullRequest_StoresNullReference()
    {
        // Arrange & Act
        var streamModel = new StreamModel
        {
            Operation = "GET_RECORDS",
            TableName = "table",
            Request = null
        };

        // Assert
        Assert.Null(streamModel.Request);
    }

    [Fact]
    public void StreamModel_ShardIteratorUpdate_UpdatesCorrectly()
    {
        // Arrange
        var streamModel = new StreamModel
        {
            ShardIterator = "old-iterator"
        };

        // Act
        streamModel.ShardIterator = "new-iterator";

        // Assert
        Assert.Equal("new-iterator", streamModel.ShardIterator);
    }
}

public class QueryModelTests
{
    [Fact]
    public void QueryModel_CreateWithProperties_AllPropertiesSet()
    {
        // Arrange
        var queryRequest = new QueryRequest
        {
            TableName = "users",
            KeyConditionExpression = "id = :id",
            Limit = 10
        };

        // Act
        var queryModel = new QueryModel
        {
            Operation = "QUERY",
            Request = queryRequest
        };

        // Assert
        Assert.Equal("QUERY", queryModel.Operation);
        Assert.NotNull(queryModel.Request);
        Assert.Equal("users", queryModel.Request.TableName);
        Assert.Equal("id = :id", queryModel.Request.KeyConditionExpression);
        Assert.Equal(10, queryModel.Request.Limit);
    }

    [Fact]
    public void QueryModel_NullRequest_Allowed()
    {
        // Arrange & Act
        var queryModel = new QueryModel
        {
            Operation = "QUERY",
            Request = null
        };

        // Assert
        Assert.Null(queryModel.Request);
    }
}

public class ScanModelTests
{
    [Fact]
    public void ScanModel_CreateWithProperties_AllPropertiesSet()
    {
        // Arrange
        var scanRequest = new ScanRequest
        {
            TableName = "products",
            Limit = 20,
            FilterExpression = "attribute_exists(price)"
        };

        // Act
        var scanModel = new ScanModel
        {
            Operation = "SCAN",
            Request = scanRequest
        };

        // Assert
        Assert.Equal("SCAN", scanModel.Operation);
        Assert.NotNull(scanModel.Request);
        Assert.Equal("products", scanModel.Request.TableName);
        Assert.Equal(20, scanModel.Request.Limit);
    }

    [Fact]
    public void ScanModel_WithFilterExpression_StoresCorrectly()
    {
        // Arrange
        var scanRequest = new ScanRequest
        {
            TableName = "inventory",
            FilterExpression = "#status <> :status"
        };

        // Act
        var scanModel = new ScanModel { Request = scanRequest };

        // Assert
        Assert.Equal("#status <> :status", scanModel.Request.FilterExpression);
    }

    [Fact]
    public void ScanModel_UpdateOperation_Works()
    {
        // Arrange
        var scanModel = new ScanModel { Operation = "SCAN" };

        // Act
        scanModel.Operation = "SCAN_UPDATED";

        // Assert
        Assert.Equal("SCAN_UPDATED", scanModel.Operation);
    }
}
