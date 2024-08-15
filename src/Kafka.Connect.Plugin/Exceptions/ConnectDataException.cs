using System;

namespace Kafka.Connect.Plugin.Exceptions;

public class ConnectDataException(string reason, Exception innerException)
    : ConnectException(reason, innerException);
