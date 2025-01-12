using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

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
            new PlainExecutor(),
            new GroupExecutor(),
            new PlainDistinctExecutor(),
        };
        public List<ISearchExecutor> searchExecutor = defaultSearchExecutor;

        public virtual ISearchExecutor GetSearchExecutor(QueryExecutorArgument arg)
        {
            return searchExecutor.FirstOrDefault(m => m.IsMatch(arg));
        }
        #endregion


        #region Serialize

        public virtual BsonDocument Serialize(object entity, IEntityDescriptor entityDescriptor)
        {
            return Serialize(entity, entityDescriptor.propertyType) as BsonDocument;
        }

        public virtual BsonValue Serialize(object value, IPropertyType propertyType)
        {
            switch (propertyType)
            {
                case IPropertyArrayType arrayType:
                    {
                        if (value is not IEnumerable enumerable) break;

                        var bsonArray = new BsonArray();
                        foreach (var item in enumerable)
                        {
                            bsonArray.Add(Serialize(item, arrayType.elementPropertyType));
                        }
                        return bsonArray;
                    }
                case IPropertyObjectType objectType:
                    {
                        var entity = value;
                        if (entity == null) return BsonValue.Create(null);

                        var doc = new BsonDocument();
                        objectType.properties.ForEach(propertyDescriptor =>
                        {
                            var value = propertyDescriptor.GetValue(entity);
                            doc.Set(propertyDescriptor.columnName, Serialize(value, propertyDescriptor.propertyType));
                        });

                        return doc;
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
            return (Entity)Deserialize(doc, entityDescriptor.propertyType);
        }

        public virtual object Deserialize(BsonDocument doc, IEntityDescriptor entityDescriptor)
        {
            return Deserialize(doc, entityDescriptor.propertyType);
        }


        public virtual object Deserialize(BsonValue bsonValue, IPropertyType propertyType)
        {
            switch (propertyType)
            {
                case IPropertyArrayType arrayType:
                    {
                        if (bsonValue?.BsonType != BsonType.Array) return null;

                        var bsonArray = bsonValue.AsBsonArray;
                        var elements = bsonArray.Select(m => Deserialize(m, arrayType.elementPropertyType));
                        return arrayType.CreateArray(elements);
                    }
                case IPropertyObjectType objectType:
                    {
                        if (bsonValue?.BsonType != BsonType.Document) return null;

                        var bsonDoc = bsonValue.AsBsonDocument;
                        var clrType = objectType.type;

                        if (bsonDoc == null) return TypeUtil.GetDefaultValue(clrType);

                        var entity = Activator.CreateInstance(clrType);
                        objectType.properties?.ForEach(propertyDescriptor =>
                        {
                            if (!bsonDoc.TryGetValue(propertyDescriptor.columnName, out var bsonValue)) return;
                            var propertyValue = Deserialize(bsonValue, propertyDescriptor.propertyType);
                            propertyDescriptor.SetValue(entity, propertyValue);
                        });

                        return entity;
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