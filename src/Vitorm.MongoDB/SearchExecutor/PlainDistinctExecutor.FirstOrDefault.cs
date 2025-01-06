using System;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class PlainDistinctExecutor
    {
        public ResultEntity FirstOrDefault<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));

            if (combinedStream.method.Contains("Last"))
            {
                if (combinedStream.skip.HasValue)
                {
                    combinedStream.skip = combinedStream.skip.Value + (combinedStream.take ?? 0) - 1;
                }
                else
                {
                    arg.ReverseOrder();
                }
            }
            if (combinedStream.take != 0)
                combinedStream.take = 1;


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
                    ["Method"] = arg.combinedStream.method,
                    ["combinedStream"] = combinedStream,
                }))
            );

            var nullable = combinedStream.method.Contains("OrDefault");
            if (combinedStream.take == 0)
            {
                return nullable ? default : throw new InvalidOperationException("Sequence contains no elements");
            }


            var collection = dbContext.dbConfig.GetDatabase().GetCollection<BsonDocument>(entityDescriptor.tableName);
            using var cursor = dbContext.session == null ? collection.Aggregate<BsonDocument>(pipeline) : collection.Aggregate<BsonDocument>(dbContext.session, pipeline);


            // read entity
            BsonDocument document = nullable ? cursor?.FirstOrDefault() : cursor.First();
            if (document == null) return default;

            var group = document?["_id"].AsBsonDocument;
            return (ResultEntity)entityReader.ReadEntity(group);
        }

    }
}
