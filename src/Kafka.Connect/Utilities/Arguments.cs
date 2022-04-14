using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Kafka.Connect.Utilities
{
    public class Arguments
    {
        private readonly IDictionary<string, List<string>> _parameters;

        public static Arguments Parse(IEnumerable<string> args) => new(args);
        
        public bool TryGetValue(string key, out string[] value)
        {
            value = null;
            if (!_parameters.ContainsKey(key) || _parameters[key] == null) return false;
            value = _parameters[key].ToArray();
            return true;
        }

        private Arguments(IEnumerable<string> args)
        {
            _parameters = new Dictionary<string, List<string>>();
            var splitter = new Regex(@"^-{1,2}|^/|=|:",
                RegexOptions.IgnoreCase | RegexOptions.Compiled);

            var remover = new Regex(@"^['""]?(.*?)['""]?$",
                RegexOptions.IgnoreCase | RegexOptions.Compiled);

            string parameter = null;

            foreach (var txt in args)
            {
                var parts = splitter.Split(txt, 3);

                switch (parts.Length)
                {
                    case 1:
                        if (parameter != null)
                        {
                            parts[0] =
                                remover.Replace(parts[0], "$1");
                            if (!_parameters.ContainsKey(parameter))
                            {
                                _parameters.Add(parameter, new List<string> {parts[0]} );
                            }
                            else
                            {
                                _parameters[parameter]?.Add(parts[0]);
                            }

                            parameter = null;
                        }

                        break;

                    case 2:
                        if (parameter != null)
                        {
                            if (!_parameters.ContainsKey(parameter))
                                _parameters.Add(parameter, new List<string> { "true"});
                        }

                        parameter = parts[1];
                        break;

                    case 3:
                        if (parameter != null)
                        {
                            if (!_parameters.ContainsKey(parameter))
                                _parameters.Add(parameter, new List<string> { "true"});
                        }

                        parameter = parts[1];
                        parts[2] = remover.Replace(parts[2], "$1");
                        if (!_parameters.ContainsKey(parameter))
                        {
                            _parameters.Add(parameter, new List<string> { parts[2]});
                        }
                        else
                        {
                            _parameters[parameter]?.Add(parts[2]);
                        }

                        parameter = null;
                        break;
                }
            }

            if (parameter == null) return;
            if (!_parameters.ContainsKey(parameter))
                _parameters.Add(parameter, new List<string> {"true"});
        }
    }
}