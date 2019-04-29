using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerVersionControl
{
    class Configuration
    {
        public Connection Connection { get; set; }
        public DateTime? LastSyncTime { get; set; }
    }

    class Connection
    {
        public string Server { get; set; }
        public string Database { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
    }
}
