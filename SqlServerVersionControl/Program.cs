using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerVersionControl
{
    class Program
    {
        static void Main(string[] args)
        {
            string configFile = "config.json";
            if (!File.Exists(configFile))
            {
                Console.WriteLine("error: " + configFile + " does not exist");
                return;
            }
            string json = File.ReadAllText(configFile);
            Configuration config = JsonConvert.DeserializeObject<Configuration>(json);
            DbContext db = new DbContext(config.Connection.Server, config.Connection.User, config.Connection.Password, config.Connection.Database);

            DateTime syncTime;
            if (config.LastSyncTime == null)
                syncTime = db.Load();
            else
                syncTime = db.Load(config.LastSyncTime.GetValueOrDefault());
            db.Sync();

            config.LastSyncTime = syncTime;

            // Serialize config
            JsonSerializer serializer = new JsonSerializer
            {
                ContractResolver = new DefaultContractResolver { NamingStrategy = new CamelCaseNamingStrategy() },
            };
            using (StreamWriter sw = new StreamWriter(configFile))
            using (JsonWriter jw = new JsonTextWriter(sw) {
                Formatting = Formatting.Indented,
                Indentation = 4
            })
            {
                serializer.Serialize(jw, config);
            }
        }
    }
}
