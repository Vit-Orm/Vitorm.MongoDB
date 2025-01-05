using System;
using System.Collections.Generic;
using System.Linq;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;


namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class PlainDistinctExecutor
    {

        public List<ResultEntity> ToList<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            return ReadList<Entity, ResultEntity>(arg).ToList();
        }

        IEnumerable<ResultEntity> ReadList<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));


            var entityReader = new EntityReader.EntityReader();
            entityReader.Init(dbContext, typeof(Entity), combinedStream.select.fields);


            var pipeline = GetPipeline(arg, entityReader);

            // Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: pipeline.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = arg.combinedStream.method ?? "ToList",
                    ["combinedStream"] = combinedStream,
                }))
            );

            if (arg.combinedStream.take == 0) yield break;

            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>(entityDescriptor.tableName);
            using var cursor = dbContext.session == null ? collection.Aggregate<BsonDocument>(pipeline) : collection.Aggregate<BsonDocument>(dbContext.session, pipeline);

            while (cursor.MoveNext())
            {
                foreach (BsonDocument document in cursor.Current)
                {
                    var group = document["_id"].AsBsonDocument;
                    yield return (ResultEntity)entityReader.ReadEntity(group);
                }
            }
        }



    }
}
