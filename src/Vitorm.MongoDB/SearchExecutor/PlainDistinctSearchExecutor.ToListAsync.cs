using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using MongoDB.Bson;

using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;


namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class PlainDistinctSearchExecutor
    {
        public async Task<List<ResultEntity>> ToListAsync<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));

            var entityReader = new EntityReader.EntityReader();
            entityReader.Init(dbContext, typeof(Entity), combinedStream.select.fields);

            var pipeline = GetPipeline<Entity, ResultEntity>(arg, entityReader);


            // Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: pipeline.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = arg.combinedStream.method ?? "ToListAsync",
                    ["combinedStream"] = combinedStream,
                }))
            );


            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>(entityDescriptor.tableName);
            using var cursor = dbContext.session == null ? await collection.AggregateAsync<BsonDocument>(pipeline) : await collection.AggregateAsync<BsonDocument>(dbContext.session, pipeline);


            var list = new List<ResultEntity>();
            while (await cursor.MoveNextAsync())
            {
                foreach (BsonDocument document in cursor.Current)
                {
                    var group = document["_id"].AsBsonDocument;
                    list.Add((ResultEntity)entityReader.ReadEntity(group));
                }
            }
            return list;
        }

    }
}
