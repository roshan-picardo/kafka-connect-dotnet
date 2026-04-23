using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.Connect.Plugin.Exceptions;
using Kafka.Connect.Plugin.Extensions;
using Kafka.Connect.Plugin.Models;
using Xunit;

namespace UnitTests.Kafka.Connect.Plugin.Extensions;

public class ParallelExtensionsTests
{
    [Fact]
    public async Task ForEachAsync_WhenAbortedRecordEncountered_StopsProcessingRemainingItems()
    {
        var records = new[]
        {
            CreateRecord(Status.Consumed),
            CreateRecord(Status.Aborted),
            CreateRecord(Status.Consumed)
        };
        var processed = 0;

        await records.ForEachAsync(CreateOptions(), record =>
        {
            processed++;
            record.Status = Status.Processed;
            return Task.CompletedTask;
        });

        Assert.Equal(1, processed);
        Assert.Equal(Status.Processed, records[0].Status);
        Assert.Equal(Status.Aborted, records[1].Status);
        Assert.Equal(Status.Consumed, records[2].Status);
    }

    [Fact]
    public async Task ForEachAsync_WhenNonConnectExceptionThrown_WrapsAsConnectDataExceptionAndAbortsForNoneTolerance()
    {
        var record = CreateRecord(Status.Consumed);

        await new[] { record }.ForEachAsync(
            CreateOptions(errorTolerance: (All: false, Data: false, None: true)),
            _ => throw new InvalidOperationException("boom"));

        Assert.Equal(Status.Aborted, record.Status);
        Assert.IsType<ConnectDataException>(record.Exception);
    }

    [Fact]
    public async Task ForEachAsync_WhenRetryableExceptionExhaustsAttempts_MarksRecordAsFailed()
    {
        var record = CreateRecord(Status.Consumed);

        await new[] { record }.ForEachAsync(
            CreateOptions(attempts: 1),
            _ => throw new ConnectRetriableException("retry", new Exception("inner")));

        Assert.Equal(Status.Failed, record.Status);
        Assert.IsType<ConnectDataException>(record.Exception);
    }

    [Fact]
    public async Task ForEachAsync_WhenExceptionMatchesConfiguredRetryableType_MarksFailedAfterAttemptsExhausted()
    {
        var record = CreateRecord(Status.Consumed);

        await new[] { record }.ForEachAsync(
            CreateOptions(attempts: 1, exceptions: [typeof(InvalidOperationException).FullName!]),
            _ => throw new InvalidOperationException("retryable-by-name"));

        Assert.Equal(Status.Failed, record.Status);
        Assert.IsType<ConnectDataException>(record.Exception);
    }

    [Fact]
    public void ForEach_WhenSourceProvided_ExecutesActionForEachItem()
    {
        var values = new List<int>();

        new[] { 1, 2, 3 }.ForEach(values.Add);

        Assert.Equal([1, 2, 3], values);
    }

    private static ConnectRecord CreateRecord(Status status)
    {
        return new ConnectRecord("orders", 0, 1)
        {
            Status = status
        };
    }

    private static ParallelRetryOptions CreateOptions(
        int attempts = 2,
        (bool All, bool Data, bool None)? errorTolerance = null,
        IList<string> exceptions = null)
    {
        return new ParallelRetryOptions
        {
            Attempts = attempts,
            DegreeOfParallelism = 1,
            Exceptions = exceptions ?? [],
            ErrorTolerance = errorTolerance ?? (true, false, false)
        };
    }
}