using System;

using MongoDB.Bson;

namespace Vitorm.MongoDB.EntityReader
{
    public interface IArgReader
    {
        string fieldPath { get; }
        string argName { get; }
        Type entityType { get; }
        object Read(BsonDocument reader);
    }

}
