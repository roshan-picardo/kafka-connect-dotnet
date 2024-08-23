namespace Kafka.Connect.Configurations
{
    public enum ErrorTolerance
    {
        None, // No errors are tolerated
        Data, // only data errors are tolerated 
        All // all errors are tolerated
    }
}