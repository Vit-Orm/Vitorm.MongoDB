using System;
using System.Collections.Generic;

using MongoDB.Driver;

namespace Vitorm.MongoDB
{
    public class DbConfig
    {
        public DbConfig(string database, string connectionString, string readOnlyConnectionString = null, int? commandTimeout = null)
        {
            this.database = database;
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
        public string database { get; protected set; }

        public string connectionString { get; protected set; }
        public string readOnlyConnectionString { get; protected set; }
        public int? commandTimeout { get; protected set; }

        protected MongoClient client;
        protected MongoClient readOnlyClient;

        public virtual MongoClient Client => client ??= new MongoClient(connectionString);
        public virtual MongoClient ReadOnlyClient => readOnlyClient ??= new MongoClient(readOnlyConnectionString);

        public virtual IMongoDatabase GetDatabase() => Client.GetDatabase(database);
        public virtual IMongoDatabase GetReadOnlyDatabase() => ReadOnlyClient.GetDatabase(database);


        public virtual DbConfig WithDatabase(string databaseName)
        {
            return new(databaseName, connectionString, readOnlyConnectionString, commandTimeout);
        }


        internal string dbHashCode => connectionString.GetHashCode().ToString() + "::" + database;



        /// <summary>
        /// to identify whether contexts are from the same database
        /// </summary>
        public virtual string dbGroupName => "MongoDb_DbSet_" + dbHashCode;
    }
}
