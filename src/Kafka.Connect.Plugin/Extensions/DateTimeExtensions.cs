using System;

namespace Kafka.Connect.Plugin.Extensions;

public static class DateTimeExtensions
{
    public static long ToUnixMicroseconds(this DateTime dateTime) =>
        (long)(dateTime - DateTime.UnixEpoch).TotalMicroseconds;
}