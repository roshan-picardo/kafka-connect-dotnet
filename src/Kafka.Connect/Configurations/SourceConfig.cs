using System.Collections.Generic;
using System.Linq;

namespace Kafka.Connect.Configurations;

public class SourceConfig
{
    private readonly IDictionary<string, CommandConfig> _commands;
    public string Topic { get; internal set; }
    public string Plugin { get; set; }
    public string Handler { get; set; }
    public int TimeOutInMs { get; set; }
    public IDictionary<string, CommandConfig> Commands
    {
        get
        {
            if (_commands == null || !_commands.Any())
            {
                return _commands;
            }
            foreach (var (topic, data) in _commands)
            {
                if (data != null && string.IsNullOrWhiteSpace(data.Topic))
                {
                    data.Topic = topic;
                }
            }
            return _commands;
        }
        init => _commands = value;
    }
}
