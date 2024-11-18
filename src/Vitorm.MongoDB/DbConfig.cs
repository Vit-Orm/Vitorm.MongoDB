using System;
using System.Collections.Generic;

using MongoDB.Driver;

namespace Vitorm.MongoDB
{
    public class DbConfig
    {
        public DbConfig(string connectionString, int? commandTimeout = null)
        {
            this.connectionString = connectionString;
            this.commandTimeout = commandTimeout;
        }
        public DbConfig(string connectionString, string readOnlyConnectionString, int? commandTimeout = null)
        {
            this.connectionString = connectionString;
            this.readOnlyConnectionString = readOnlyConnectionString;
            this.commandTimeout = commandTimeout;
        }
        public DbConfig(Dictionary<string, object> config)
        {
            object value;
            if (config.TryGetValue("connectionString", out value))
                this.connectionString = value as string;

            if (config.TryGetValue("readOnlyConnectionString", out value))
                this.readOnlyConnectionString = value as string;


            if (config.TryGetValue("database", out value))
                this.database = value as string;


            if (config.TryGetValue("commandTimeout", out value) && value is Int32 commandTimeout)
                this.commandTimeout = commandTimeout;
        }
        public string database { get; set; }

        public string connectionString { get; set; }
        public string readOnlyConnectionString { get; set; }
        public int? commandTimeout { get; set; }

        MongoClient client;
        MongoClient readOnlyClient;

        MongoClient Client => client ??= new MongoClient(connectionString);
        MongoClient ReadOnlyClient => readOnlyClient ??= new MongoClient(readOnlyConnectionString);

        public IMongoDatabase GetDatabase() => Client.GetDatabase(database);
        public IMongoDatabase GetReadOnlyDatabase() => ReadOnlyClient.GetDatabase(database);




        internal string dbHashCode => connectionString.GetHashCode().ToString();



        /// <summary>
        /// to identify whether contexts are from the same database
        /// </summary>
        public virtual string dbGroupName => "DbSet_" + dbHashCode;
    }
}
