using System;
using System.Linq;

using MongoDB.Bson;

using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    /// <summary>
    /// Queryable.Count or Queryable_Extensions.TotalCount
    /// </summary>
    public partial class Count : IQueryExecutor
    {
        public static readonly Count Instance = new();

        public string methodName => nameof(Queryable.Count);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            using var _ = execArg;

            return Execute(execArg);
        }


        public static int Execute(QueryExecutorArgument execArg)
        {
            CombinedStream combinedStream = execArg.combinedStream;
            var dbContext = execArg.dbContext;
            var translateService = dbContext.translateService;

            // #2 filter
            var filter = translateService.TranslateFilter(execArg, combinedStream);

            IQueryable query = null;

            if (combinedStream.source is SourceStream sourceStream)
            {
                query = sourceStream.GetSource() as IQueryable;
            }
            else if (combinedStream.source is CombinedStream baseStream)
            {
                query = (baseStream.source as SourceStream)?.GetSource() as IQueryable;

            }

            var queryEntityType = query?.ElementType;
            var entityDescriptor = dbContext.GetEntityDescriptor(queryEntityType);


            // Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: filter.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "Count",
                    ["combinedStream"] = combinedStream,
                }))
            );


            // #3 execute query
            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>(entityDescriptor.tableName);
            var count = (int)collection.CountDocuments(filter);


            // Count and TotalCount
            if (count > 0 && combinedStream.method == nameof(Queryable.Count))
            {
                if (combinedStream.skip > 0) count = Math.Max(count - combinedStream.skip.Value, 0);

                if (combinedStream.take.HasValue)
                    count = Math.Min(count, combinedStream.take.Value);
            }

            return count;
        }

    }
}
