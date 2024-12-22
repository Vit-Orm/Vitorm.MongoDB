using System;
using System.Linq;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    public partial class ToExecuteString : IQueryExecutor
    {
        public static readonly ToExecuteString Instance = new();

        public string methodName => nameof(Orm_Extensions.ToExecuteString);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            using var _ = execArg;

            // #1
            CombinedStream combinedStream = execArg.combinedStream;
            var dbContext = execArg.dbContext;
            var translateService = dbContext.translateService;

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


            // #2 filter
            var filter = translateService.TranslateFilter(execArg, combinedStream);

            // #3 sortDoc
            BsonDocument sortDoc = null;
            if (combinedStream.orders?.Any() == true)
            {
                sortDoc = new BsonDocument();
                combinedStream.orders.ForEach(item =>
                {
                    var fieldPath = translateService.GetFieldPath(execArg, item.member);
                    sortDoc.Add(fieldPath, BsonValue.Create(item.asc ? 1 : -1));
                });
            }


            // #4 Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: filter.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "ToExecuteString",
                    ["sortDoc"] = sortDoc,
                    ["combinedStream"] = combinedStream,
                }))
            );


            return filter.ToJson();
        }

    }

}