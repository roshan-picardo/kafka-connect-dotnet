using System;

namespace Kafka.Connect.Plugin.Exceptions;

public class ConnectRetriableException(string reason, Exception innerException)
    : ConnectException(reason, innerException);
