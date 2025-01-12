using System;

using MongoDB.Bson;

using Vitorm.Entity.PropertyType;

namespace Vitorm.MongoDB.EntityReader
{
    class ModelReader : IArgReader
    {
        public string argName { get; set; }
        public string fieldPath { get; set; }
        public Type entityType { get; }
        IPropertyType propertyType;
        DbContext dbContext;
        public ModelReader(DbContext dbContext, IPropertyType propertyType, Type entityType, string fieldPath, string argName)
        {
            this.dbContext = dbContext;
            this.entityType = entityType;
            this.propertyType = propertyType;

            this.fieldPath = fieldPath;
            this.argName = argName;
        }

        public object Read(BsonDocument reader)
        {
            var bsonValue = reader[argName];
            return dbContext.Deserialize(bsonValue, propertyType);
        }

    }



}
