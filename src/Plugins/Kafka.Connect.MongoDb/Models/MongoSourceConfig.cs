using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Web;
using Kafka.Connect.Plugin.Models;

namespace Kafka.Connect.MongoDb.Models
{
    public class MongoSourceConfig
    {
        private string _connectionUri;

        public string ConnectionUri
        {
            get => _connectionUri == null
                ? null
                : string.Format(_connectionUri, Username,
                    Password == null
                        ? Password
                        : HttpUtility.UrlEncode(Encoding.UTF8.GetString(Convert.FromBase64String(Password))),
                    Database);
            set => _connectionUri = value;
        }

        public string Database { get; set; }

        public string Username { get; set; }

        public string Password { get; set; }
        
        public IDictionary<string, CommandConfig> Commands { get; set; }
    }

    public class CommandConfig : Command
    {
        public string Collection { get; set; }
        public string TimestampColumn { get; set; }
        public string[] KeyColumns { get; set; }
        
        public override JsonNode ToJson() => JsonSerializer.SerializeToNode(this);
    }
}