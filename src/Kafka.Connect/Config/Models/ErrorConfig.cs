namespace Kafka.Connect.Config.Models
{
    public class ErrorConfig
    {
        public ErrorTolerance Tolerance { get; init; }
        public DeadLetterConfig DeadLetter { get; set; }
    }
}