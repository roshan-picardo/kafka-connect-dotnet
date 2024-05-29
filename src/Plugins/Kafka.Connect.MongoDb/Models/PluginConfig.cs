using System;
using System.Text;
using System.Web;

namespace Kafka.Connect.MongoDb.Models;

public class PluginConfig
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
    public string Password { get; set; }
    public string Username { get; set; }
}