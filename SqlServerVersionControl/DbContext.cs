using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerVersionControl
{
    class DbContext
    {
        private Server srv;
        private string DatabaseName;
        private DateTime SyncTime;

        // TODO: move these dir names to config
        private static string TableDir = "Tables";
        private static string ViewDir = "Views";
        private static string StoredProcedureDir = "StoredProcedures";
        private static string FunctionDir = "Functions";
        private static string ScriptFileExtension = ".sql";

        private List<string> TableNames;
        private List<string> ViewNames;
        private List<string> StoredProcedureNames;
        private List<string> FunctionNames;
        private List<DbObject> UpdatedTables;
        private List<DbObject> UpdatedViews;
        private List<DbObject> UpdatedStoredProcedures;
        private List<DbObject> UpdatedFunctions;

        public DbContext(string dataSource, string user, string password, string dbName)
        {
            // Set up the connection using SQL Server Authentication  
            srv = new Server(dataSource);
            srv.ConnectionContext.LoginSecure = false;   // set to true for Windows Authentication
            srv.ConnectionContext.Login = user;
            srv.ConnectionContext.Password = password;
            srv.ConnectionContext.DatabaseName = dbName;
            DatabaseName = dbName;
        }

        public DateTime Load()
        {
            return Load(new DateTime(1753, 1, 1));
        }

        public DateTime Load(DateTime lastSyncTimestamp)
        {
            DataTable allObj = new DataTable();
            DataTable updatedObj = new DataTable();
            int numObj = 0;

            using (SqlConnection conn = new SqlConnection(srv.ConnectionContext.ConnectionString))
            {
                conn.Open();
                SqlCommand cmd = new SqlCommand("select getdate()", conn);
                SqlDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                    SyncTime = reader.GetDateTime(0);
                cmd.CommandText =
                    "select o.name, s.name [schema], o.type, case when o.modify_date > '"
                    + lastSyncTimestamp.ToString("yyyy-MM-dd HH:mm:ss.fff") + "' then o.create_date end as create_date, case when o.modify_date > '"
                    + lastSyncTimestamp.ToString("yyyy-MM-dd HH:mm:ss.fff") + "' then o.modify_date end as modify_date from sys.objects o inner join sys.schemas s on o.schema_id = s.schema_id where is_ms_shipped = 0 and [type] in ('U','V','P','FN','IF','TF') and o.modify_date <= '" + SyncTime + "'";
                reader.Close();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(allObj);
                numObj = allObj.Rows.Count;
            }
            TableNames = new List<string>();
            ViewNames = new List<string>();
            StoredProcedureNames = new List<string>();
            FunctionNames = new List<string>();
            UpdatedTables = new List<DbObject>();
            UpdatedViews = new List<DbObject>();
            UpdatedStoredProcedures = new List<DbObject>();
            UpdatedFunctions = new List<DbObject>();
            foreach (DataRow row in allObj.AsEnumerable())
            {
                string name = (string)row[0];
                string schema = (string)row[1];
                string type = ((string)row[2]).TrimEnd();
                bool isUpdated = !row.IsNull(3);
                switch (type)
                {
                    case "U":
                        if (isUpdated)
                            UpdatedTables.Add(new DbObject(name, schema, TableDir, ScriptFileExtension));
                        TableNames.Add(Path.Combine(Environment.CurrentDirectory, TableDir, schema + "." + name + ScriptFileExtension));
                        break;
                    case "V":
                        if (isUpdated)
                            UpdatedViews.Add(new DbObject(name, schema, ViewDir, ScriptFileExtension));
                        ViewNames.Add(Path.Combine(Environment.CurrentDirectory, ViewDir, schema + "." + name + ScriptFileExtension));
                        break;
                    case "P":
                        if (isUpdated)
                            UpdatedStoredProcedures.Add(new DbObject(name, schema, StoredProcedureDir, ScriptFileExtension));
                        StoredProcedureNames.Add(Path.Combine(Environment.CurrentDirectory, StoredProcedureDir, schema + "." + name + ScriptFileExtension));
                        break;
                    case "FN":
                    case "IF":
                    case "TF":
                        if (isUpdated)
                            UpdatedFunctions.Add(new DbObject(name, schema, FunctionDir, ScriptFileExtension));
                        FunctionNames.Add(Path.Combine(Environment.CurrentDirectory, FunctionDir, schema + "." + name + ScriptFileExtension));
                        break;
                    default:
                        break;
                }
            }
            return SyncTime;
        }

        public void Sync()
        {
            // Display new/updated object count
            int updateCount = UpdatedTables.Count + UpdatedViews.Count + UpdatedStoredProcedures.Count + UpdatedFunctions.Count;
            if (updateCount == 0)
                Console.WriteLine("No new/updated objects found.");
            else
            {
                Console.WriteLine("Found " + updateCount + " new/updated object" + (updateCount == 1 ? "": "s") + ":");
                Console.WriteLine("Tables - " + UpdatedTables.Count);
                Console.WriteLine("Views - " + UpdatedViews.Count);
                Console.WriteLine("Stored Procedures - " + UpdatedStoredProcedures.Count);
                Console.WriteLine("Functions - " + UpdatedFunctions.Count);
                Console.WriteLine();
            }

            // Check which objects no longer exist in db
            List<string> tblFilenames = new List<string>();
            if (Directory.Exists(TableDir))
                tblFilenames = Directory.EnumerateFiles(TableDir, "*" + ScriptFileExtension, SearchOption.TopDirectoryOnly).Select(f => Path.GetFullPath(f)).ToList();
            List<string> viewFilenames = new List<string>();
            if (Directory.Exists(ViewDir))
                viewFilenames = Directory.EnumerateFiles(ViewDir, "*" + ScriptFileExtension, SearchOption.TopDirectoryOnly).Select(f => Path.GetFullPath(f)).ToList();
            List<string> spFilenames = new List<string>();
            if (Directory.Exists(StoredProcedureDir))
                spFilenames = Directory.EnumerateFiles(StoredProcedureDir, "*" + ScriptFileExtension, SearchOption.TopDirectoryOnly).Select(f => Path.GetFullPath(f)).ToList();
            List<string> fnFilenames = new List<string>();
            if (Directory.Exists(FunctionDir))
                fnFilenames = Directory.EnumerateFiles(FunctionDir, "*" + ScriptFileExtension, SearchOption.TopDirectoryOnly).Select(f => Path.GetFullPath(f)).ToList();

            var delTables = tblFilenames.Except(TableNames).ToList();
            var delViews = viewFilenames.Except(ViewNames).ToList();
            var delStoredProcedures = spFilenames.Except(StoredProcedureNames).ToList();
            var delFunctions = fnFilenames.Except(FunctionNames).ToList();

            // Display deleted object count
            int delCount = delTables.Count + delViews.Count + delStoredProcedures.Count + delFunctions.Count;
            if (delCount == 0)
                Console.WriteLine("No deleted objects found.");
            else
            {
                Console.WriteLine(delCount == 1 ? "Found 1 deleted object:" : "Found " + delCount + " deleted objects:");
                if (delTables.Count > 0)
                    Console.WriteLine("Tables - " + delTables.Count);
                if (delViews.Count > 0)
                    Console.WriteLine("Views - " + delViews.Count);
                if (delStoredProcedures.Count > 0)
                    Console.WriteLine("Stored Procedures - " + delStoredProcedures.Count);
                if (delFunctions.Count > 0)
                    Console.WriteLine("Functions - " + delFunctions.Count);
            }

            if (updateCount == 0 && delCount == 0)
                return;
            Console.WriteLine();

            if (updateCount > 0)
            {
                // Create script directories if they don't already exist
                Directory.CreateDirectory(TableDir);
                Directory.CreateDirectory(ViewDir);
                Directory.CreateDirectory(StoredProcedureDir);
                Directory.CreateDirectory(FunctionDir);

                // Define a Scripter object and set the required scripting options.   
                Scripter scrp = new Scripter(srv);
                scrp.Options.ScriptDrops = false;
                scrp.Options.Indexes = true;  // include indexes  
                scrp.Options.DriAllConstraints = true;  // include referential constraints
                scrp.Options.NoCollation = true;
                scrp.Options.ToFileOnly = true;

                Database db = srv.Databases[DatabaseName];

                // Loop updated tables and script them
                foreach (var o in UpdatedTables)
                {
                    Console.WriteLine("Scripting table '" + o.FullName + "'...");
                    var table = db.Tables[o.Name, o.Schema];
                    scrp.Options.FileName = o.ScriptFullPath;  // set output path of the scripter
                    scrp.Script(new Urn[] { table.Urn });
                }

                // Loop updated views and script them
                foreach (var o in UpdatedViews)
                {
                    Console.WriteLine("Scripting view '" + o.FullName + "'...");
                    var view = db.Views[o.Name, o.Schema];
                    scrp.Options.FileName = o.ScriptFullPath;  // set output path of the scripter
                    scrp.Script(new Urn[] { view.Urn });
                }

                // Loop updated stored procedures and script them
                foreach (var o in UpdatedStoredProcedures)
                {
                    Console.WriteLine("Scripting stored procedure '" + o.FullName + "'...");
                    var storedProcedure = db.StoredProcedures[o.Name, o.Schema];
                    scrp.Options.FileName = o.ScriptFullPath;  // set output path of the scripter
                    scrp.Script(new Urn[] { storedProcedure.Urn });
                }

                // Loop updated functions and script them
                foreach (var o in UpdatedFunctions)
                {
                    Console.WriteLine("Scripting function '" + o.FullName + "'...");
                    var function = db.UserDefinedFunctions[o.Name, o.Schema];
                    scrp.Options.FileName = o.ScriptFullPath;  // set output path of the scripter
                    scrp.Script(new Urn[] { function.Urn });
                }
            }

            // Delete local scripts
            if (delCount > 0)
            {
                delTables.ForEach(f =>
                {
                    Console.WriteLine("Deleting '" + f + "'...");
                    File.Delete(f);
                });
                delViews.ForEach(f =>
                {
                    Console.WriteLine("Deleting '" + f + "'...");
                    File.Delete(f);
                });
                delStoredProcedures.ForEach(f =>
                {
                    Console.WriteLine("Deleting '" + f + "'...");
                    File.Delete(f);
                });
                delFunctions.ForEach(f =>
                {
                    Console.WriteLine("Deleting '" + f + "'...");
                    File.Delete(f);
                });
            }
        }
    }

    class DbObject
    {
        public string Name { get; set; }
        public string Schema { get; set; }
        public string FullName { get; set; }
        public string ScriptFullPath { get; set; }
        public string Script { get; set; }

        public DbObject(string name, string schema, string scriptDirectory, string scriptFileExtension)
        {
            Name = name;
            Schema = schema;
            FullName = schema + "." + name;
            ScriptFullPath = Path.Combine(Environment.CurrentDirectory, scriptDirectory, FullName + scriptFileExtension);
        }

        public DbObject(string name, string schema, string scriptDirectory, string scriptFileExtension, string script)
            : this(name, schema, scriptDirectory, scriptFileExtension)
        {
            Script = script;
        }
    }
}
