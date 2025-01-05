using System;
using System.Linq;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class PlainExecutor
    {
        public string ToExecuteString(QueryExecutorArgument arg, Type entityType)
        {
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var translateService = dbContext.translateService;


            // #2 filter
            var filter = translateService.TranslateFilter(arg, combinedStream);


            // #3 sortDoc
            BsonDocument sortDoc = null;
            if (combinedStream.orders?.Any() == true)
            {
                sortDoc = new BsonDocument();
                combinedStream.orders.ForEach(item =>
                {
                    var fieldPath = translateService.GetFieldPath(arg, item.member);
                    sortDoc.Add(fieldPath, BsonValue.Create(item.asc ? 1 : -1));
                });
            }


            // pipeline
            var pipeline = new[]
            {
                    //new BsonDocument("$match", new BsonDocument("userId", new BsonDocument("$gt", 1))),
                    filter == null ? null : new BsonDocument("$match", filter),
                     

                    //new BsonDocument("$sort", new BsonDocument("count", -1)),
                     sortDoc == null ? null : new BsonDocument("$sort", sortDoc),

                    //new BsonDocument("$skip", 1),
                    combinedStream.skip>0 ? new BsonDocument("$skip", combinedStream.skip.Value) : null ,

                    //new BsonDocument("$limit", 5),
                    combinedStream.take>0 ? new BsonDocument("$limit", combinedStream.take.Value) : null ,

            }.Where(m => m != null).ToArray();


            // Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: pipeline.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = dbContext.GetEntityDescriptor(entityType),
                    ["Method"] = arg.combinedStream.method ?? "ToExecuteString",
                    ["combinedStream"] = arg.combinedStream,
                    ["sortDoc"] = sortDoc,
                }))
            );

            return pipeline.ToJson();
        }

    }
}
