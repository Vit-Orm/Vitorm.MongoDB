using System;

using MongoDB.Bson;

namespace Vitorm.MongoDB.EntityReader
{

    class ValueReader : IArgReader
    {
        public string argName { get; set; }

        public string fieldPath { get; set; }

        protected Type valueType { get; set; }
        protected Type underlyingType;
        public Type entityType { get => valueType; }

        public ValueReader(Type valueType, string fieldPath, string argName)
        {
            this.valueType = valueType;
            underlyingType = TypeUtil.GetUnderlyingType(valueType);

            this.fieldPath = fieldPath;
            this.argName = argName;
        }
        public object Read(BsonDocument reader)
        {
            var bsonValue = reader[argName];
            var value = BsonTypeMapper.MapToDotNetValue(bsonValue);
            return TypeUtil.ConvertToUnderlyingType(value, underlyingType);
        }
    }


}
