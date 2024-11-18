using System;
using System.Collections;
using System.Data;
using System.Linq;

using MongoDB.Bson;

using Vit.Linq;

using Vitorm.Entity;
using Vitorm.Entity.PropertyType;
using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;

namespace Vitorm.MongoDB
{
    public partial class DbContext : Vitorm.DbContext
    {
        public DbConfig dbConfig { get; protected set; }

        public DbContext(DbConfig dbConfig) : base(DbSetConstructor.CreateDbSet)
        {
            this.dbConfig = dbConfig;
        }

        public DbContext(string connectionString) : this(new DbConfig(connectionString))
        {
        }


        #region Transaction
        public virtual IDbTransaction BeginTransaction() => throw new System.NotImplementedException();
        public virtual IDbTransaction GetCurrentTransaction() => throw new System.NotImplementedException();

        #endregion



        public virtual string databaseName => throw new System.NotImplementedException();
        public virtual void ChangeDatabase(string databaseName) => throw new System.NotImplementedException();


        #region StreamReader
        public static StreamReader defaultStreamReader = new StreamReader();
        public StreamReader streamReader = defaultStreamReader;
        #endregion

        #region StreamReader
        public static TranslateService defaultTranslateService = new TranslateService();
        public TranslateService translateService = defaultTranslateService;
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