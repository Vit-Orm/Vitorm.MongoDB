﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using Vit.Linq;

using Vitorm.Entity;
using Vitorm.Entity.PropertyType;
using Vitorm.MongoDB.QueryExecutor;
using Vitorm.MongoDB.SearchExecutor;
using Vitorm.MongoDB.Transaction;
using Vitorm.StreamQuery;
using Vitorm.Transaction;

namespace Vitorm.MongoDB
{
    public partial class DbContext : Vitorm.DbContext
    {

        public override void Dispose()
        {
            try
            {
                transactionManager?.Dispose();
            }
            finally
            {
                transactionManager = null;
                base.Dispose();
            }
        }





        public DbConfig dbConfig { get; protected set; }

        public DbContext(DbConfig dbConfig) : base(DbSetConstructor.CreateDbSet)
        {
            this.dbConfig = dbConfig;
        }

        public DbContext(string database, string connectionString) : this(new DbConfig(database, connectionString))
        {
        }




        #region Transaction  
        protected virtual TransactionManager transactionManager { get; set; }

        public override ITransaction BeginTransaction()
        {
            transactionManager ??= new TransactionManager(this);
            return transactionManager.BeginTransaction();
        }
        public virtual IClientSessionHandle session => transactionManager?.session;

        #endregion


        public virtual string databaseName => dbConfig.database;
        public virtual void ChangeDatabase(string databaseName)
        {
            dbConfig = dbConfig.WithDatabase(databaseName);
        }


        #region StreamReader
        public static StreamReader defaultStreamReader = new StreamReader();
        public StreamReader streamReader = defaultStreamReader;
        #endregion

        #region TranslateService
        public static TranslateService defaultTranslateService = new TranslateService();
        public TranslateService translateService = defaultTranslateService;
        #endregion


        #region SearchExecutor
        public static List<ISearchExecutor> defaultSearchExecutor = new() {
            new PlainSearchExecutor(),
            new GroupExecutor(),
        };
        public List<ISearchExecutor> searchExecutor = defaultSearchExecutor;

        public virtual async Task<bool> ExecuteSearchAsync<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            foreach (var executor in searchExecutor)
            {
                var success = await executor.ExecuteSearchAsync<Entity, ResultEntity>(arg);
                if (success) return true;
            }
            throw new NotSupportedException("not supported Search");
        }
        public virtual bool ExecuteSearch<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            foreach (var executor in searchExecutor)
            {
                var success = executor.ExecuteSearch<Entity, ResultEntity>(arg);
                if (success) return true;
            }
            throw new NotSupportedException("not supported Search");
        }
        #endregion


        #region Serialize

        public virtual BsonDocument Serialize(object entity, IEntityDescriptor entityDescriptor)
        {
            return SerializeObject(entity, entityDescriptor.propertyType) as BsonDocument;
        }

        protected virtual BsonValue SerializeObject(object entity, IPropertyObjectType objectType)
        {
            if (entity == null) return BsonValue.Create(null);

            var doc = new BsonDocument();

            objectType.properties.ForEach(propertyDescriptor =>
            {
                var value = propertyDescriptor.GetValue(entity);
                doc.Set(propertyDescriptor.columnName, SerializeProperty(value, propertyDescriptor.propertyType));
            });

            return doc;
        }

        protected virtual BsonValue SerializeProperty(object value, IPropertyType propertyType)
        {
            switch (propertyType)
            {
                case IPropertyArrayType arrayType:
                    {
                        if (value is not IEnumerable enumerable) break;

                        var bsonArray = new BsonArray();
                        foreach (var item in enumerable)
                        {
                            bsonArray.Add(SerializeProperty(item, arrayType.elementPropertyType));
                        }
                        return bsonArray;
                    }
                case IPropertyObjectType objectType:
                    {
                        if (value == null) break;

                        return SerializeObject(value, objectType);
                    }
                case IPropertyValueType valueType:
                    {
                        return BsonValue.Create(value);
                    }
            }
            return BsonValue.Create(null);
        }

        #endregion

        #region Deserialize

        public virtual Entity Deserialize<Entity>(BsonDocument doc, IEntityDescriptor entityDescriptor)
        {
            return (Entity)Deserialize(doc, entityDescriptor);
        }

        public virtual object Deserialize(BsonDocument doc, IEntityDescriptor entityDescriptor)
        {
            return DeserializeObject(doc, entityDescriptor.entityType, entityDescriptor.properties);
        }

        protected virtual object DeserializeObject(BsonDocument doc, Type clrType, IPropertyDescriptor[] properties)
        {
            if (doc == null) return TypeUtil.GetDefaultValue(clrType);
            var entity = Activator.CreateInstance(clrType);

            properties.ForEach(propertyDescriptor =>
            {
                if (!doc.TryGetValue(propertyDescriptor.columnName, out var bsonValue)) return;
                var propertyValue = DeserializeProperty(bsonValue, propertyDescriptor.propertyType);
                propertyDescriptor.SetValue(entity, propertyValue);
            });

            return entity;
        }


        protected virtual object DeserializeProperty(BsonValue bsonValue, IPropertyType propertyType)
        {
            switch (propertyType)
            {
                case IPropertyArrayType arrayType:
                    {
                        if (bsonValue?.BsonType != BsonType.Array) return null;

                        var bsonArray = bsonValue.AsBsonArray;
                        var elements = bsonArray.Select(m => DeserializeProperty(m, arrayType.elementPropertyType));
                        return arrayType.CreateArray(elements);
                    }
                case IPropertyObjectType objectType:
                    {
                        if (bsonValue?.BsonType != BsonType.Document) return null;

                        var bsonDoc = bsonValue.AsBsonDocument;
                        return DeserializeObject(bsonDoc, objectType.type, objectType.properties);
                    }
                case IPropertyValueType valueType:
                    {
                        var value = BsonTypeMapper.MapToDotNetValue(bsonValue);
                        value = TypeUtil.ConvertToType(value, valueType.type);
                        return value;
                    }
            }
            return null;
        }

        #endregion

    }
}