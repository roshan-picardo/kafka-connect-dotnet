using System;
using System.Text;
using System.Web;
using Newtonsoft.Json;

namespace Kafka.Connect.Mongodb.Models
{
    public class Properties
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


        private CollectionConfig _collection;
        // keeping backward compatibility
        private string collection { get; set; }

        public CollectionConfig Collection
        {
            get
            {
                if (_collection == null && !string.IsNullOrWhiteSpace(collection))
                    _collection = new CollectionConfig() {Name = collection};
                return _collection;
            }
            set => _collection = value;
        }

        public string Username { get; set; }

        public string Password { get; set; }

        public Definition Definition { get; set; }
        public WriteStrategy WriteStrategy { get; set; }
    }
}