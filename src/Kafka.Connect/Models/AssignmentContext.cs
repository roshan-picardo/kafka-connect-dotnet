namespace Kafka.Connect.Models;

public class AssignmentContext
{
    public string Topic { get; set; }
    public int Partition { get; set; }
    public bool IsEof { get; set; }
    public string Name { get; set; }
}