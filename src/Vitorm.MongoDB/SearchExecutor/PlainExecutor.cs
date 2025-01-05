using System;
using System.Linq;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.Entity;
using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;


namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class PlainExecutor : ISearchExecutor
    {
        public virtual bool IsMatch(QueryExecutorArgument arg)
        {
            CombinedStream combinedStream = arg.combinedStream;

            var dbContext = arg.dbContext;

            if (combinedStream.source is not SourceStream) return false;
            if (combinedStream.isGroupedStream) return false;
            if (combinedStream.joins?.Any() == true) return false;
            if (combinedStream.distinct != null) return false;

            return true;
        }




        IFindFluent<BsonDocument, BsonDocument> ExecuteQuery<Entity, ResultEntity>(QueryExecutorArgument arg, IEntityDescriptor entityDescriptor)
        {
            // #1
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

            // #4 Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: filter.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = arg.combinedStream.method ?? "ToList",
                    ["combinedStream"] = combinedStream,
                    ["sortDoc"] = sortDoc,
                }))
            );

            if (combinedStream.take == 0) return null;

            // #5 execute query
            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>(entityDescriptor.tableName);
            var fluent = dbContext.session == null ? collection.Find(filter) : collection.Find(dbContext.session, filter);

            // #6 execute query
            if (sortDoc != null) fluent = fluent.Sort(sortDoc);

            // #7 skip take
            if (combinedStream.skip > 0) fluent = fluent.Skip(combinedStream.skip);
            if (combinedStream.take > 0) fluent = fluent.Limit(combinedStream.take);

            return fluent;
        }
    }
}
