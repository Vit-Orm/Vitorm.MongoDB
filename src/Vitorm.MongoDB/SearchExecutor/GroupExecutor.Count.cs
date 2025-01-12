using System;
using System.Linq;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.MongoDB.QueryExecutor;

namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class GroupExecutor
    {
        public int Count(QueryExecutorArgument arg, Type entityType)
        {
            var dbContext = arg.dbContext;
            var entityDescriptor = dbContext.GetEntityDescriptor(entityType);

            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>(entityDescriptor.tableName);

            var pipeline = GetPipeline(arg);
            pipeline = pipeline.Concat(new[] { new BsonDocument("$count", "count") }).ToArray();


            // Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: pipeline.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = arg.combinedStream.method ?? "Count",
                    ["combinedStream"] = arg.combinedStream,
                }))
            );


            // Execute aggregation
            using var cursor = dbContext.session == null ? collection.Aggregate<BsonDocument>(pipeline) : collection.Aggregate<BsonDocument>(dbContext.session, pipeline);

            var doc = cursor.FirstOrDefault();

            return doc?["count"].AsInt32 ?? 0;
        }







    }
}
