using System.Collections.Generic;

namespace Kafka.Connect.Plugin.Models;

public class ParallelRetryOptions
{
    public int DegreeOfParallelism { get; set; }
    public int Attempts { get; init; }
    public int Interval { get; init; }
    public bool ErrorTolerated { get; init; }
    public IList<string> Exceptions { get; init; }
    public (bool All, bool Data, bool None) ErrorTolerance { get; set; }
}
