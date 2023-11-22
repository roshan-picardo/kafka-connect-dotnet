namespace Kafka.Connect.Plugin.Extensions;

public static class TypeExtensions
{
    public static bool Is<T>(this object o) => o != null && o.GetType() == typeof(T);
    public static bool Is(this object o, string fullName) => o != null && o.GetType().FullName == fullName;
}
