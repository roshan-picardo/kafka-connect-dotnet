using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Kafka.Connect.Plugin.Exceptions;

public class ConnectToleranceExceededException(string reason, params Exception[] innerExceptions)
    : ConnectException(reason)
{
    private readonly ReadOnlyCollection<Exception> _innerExceptions = new(innerExceptions);

    public override string ToString()
    {
        return ToString(_innerExceptions);
    }

    public IEnumerable<ConnectException> GetConnectExceptions() =>
        new ReadOnlyCollection<ConnectException>(_innerExceptions.OfType<ConnectException>().ToArray());

    public IEnumerable<Exception> GetNonConnectExceptions() =>
        new ReadOnlyCollection<Exception>(_innerExceptions.Except(GetConnectExceptions()).ToArray());
}
