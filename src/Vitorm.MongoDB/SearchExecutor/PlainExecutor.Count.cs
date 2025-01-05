using System;

using MongoDB.Bson;

using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class PlainExecutor
    {
        public int Count(QueryExecutorArgument arg, Type entityType)
        {
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var entityDescriptor = dbContext.GetEntityDescriptor(entityType);
            var translateService = dbContext.translateService;

            // #2 filter
            var filter = translateService.TranslateFilter(arg, combinedStream);


            // Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: filter.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = arg.combinedStream.method ?? "Count",
                    ["combinedStream"] = combinedStream,
                }))
            );


            // #3 execute query
            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>(entityDescriptor.tableName);
            var count = (int)collection.CountDocuments(filter);

            return count;
        }







    }
}
