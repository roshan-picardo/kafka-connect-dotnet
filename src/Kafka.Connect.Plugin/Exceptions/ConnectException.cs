using System;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Text;

namespace Kafka.Connect.Plugin.Exceptions;

public class ConnectException : Exception
{
    public ConnectException() : base("Unknown")
    {
    }

    protected ConnectException(string message) : base(message)
    {
    }

    protected ConnectException(string message, Exception innerException) : base(message, innerException)
    {
    }

    public string Topic { get; private set; }
    public int Partition { get; private set; }
    public long Offset { get; private set; }

    protected string ToString(ReadOnlyCollection<Exception> innerExceptions)
    {
        var text = new StringBuilder();
        text.Append(base.ToString());

        for (var i = 0; i < innerExceptions.Count; i++)
        {
            if (innerExceptions[i] == InnerException)
                continue; 

            text.Append(Environment.NewLine).Append("--->");
            text.Append(CultureInfo.InvariantCulture, $"(Inner Exception #{i})");
            text.Append(innerExceptions[i]);
            text.Append("<---");
            text.AppendLine();
        }

        return text.ToString();
    }
}
