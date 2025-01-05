using System.Collections.Generic;
using System.Linq;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;

using static Vitorm.MongoDB.SearchExecutor.GroupExecutor;


namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class PlainDistinctSearchExecutor : ISearchExecutor
    {
        public virtual bool IsMatch(QueryExecutorArgument arg)
        {
            CombinedStream combinedStream = arg.combinedStream;

            var dbContext = arg.dbContext;

            if (combinedStream.source is not SourceStream) return false;
            if (combinedStream.isGroupedStream) return false;
            if (combinedStream.joins?.Any() == true) return false;
            if (combinedStream.distinct != true) return false;
            if (combinedStream.select == null) return false;

            return true;
        }



        public static BsonDocument[] GetPipeline<Entity, ResultEntity>(QueryExecutorArgument arg, EntityReader.EntityReader entityReader)
        {
            // #1
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var translateService = dbContext.translateService;

            // #2 filter
            var filter = combinedStream?.where == null ? null : translateService.TranslateFilter(arg, combinedStream.where);

            // #3 groupByFields
            List<(string field, string fieldAs)> groupFields = entityReader.entityArgReaders.Select(f => (f.fieldPath, f.argName)).ToList();
            var groupByFields = new BsonDocument(groupFields.ToDictionary(f => f.fieldAs, f => "$" + f.field));
            var groupFieldArg = new QueryExecutorArgument_GroupFilter(arg, groupFields);

            // #5 order by fields
            List<(string field, bool asc, int index)> orderFields = combinedStream.orders?.Select((orderField, index) =>
            {
                var field = groupFieldArg.GetFieldPath(orderField.member);
                return (field, orderField.asc, index);
            }).ToList();
            var orderByFields = orderFields == null ? null : new BsonDocument(orderFields.ToDictionary(field => field.field, field => BsonValue.Create(field.asc ? 1 : -1)));


            // Aggregation pipeline
            var pipeline = new[]
            {
                    //new BsonDocument("$match", new BsonDocument("userId", new BsonDocument("$gt", 1))),
                    filter == null ? null : new BsonDocument("$match", filter),

                    new BsonDocument("$group", new BsonDocument
                    {
                        //{ "_id", new BsonDocument { { "userFatherId", "$userFatherId" }, { "userMotherId", "$userMotherId" } } },
                        { "_id", groupByFields },
                    }),
                     

                    //new BsonDocument("$sort", new BsonDocument("count", -1)),
                     orderByFields == null ? null : new BsonDocument("$sort", orderByFields),

                    new BsonDocument("$project", new BsonDocument
                    {
                        { "_id", 1 },
                    }),

                    //new BsonDocument("$skip", 1),
                    combinedStream.skip>0 ? new BsonDocument("$skip", combinedStream.skip.Value) : null ,

                    //new BsonDocument("$limit", 5),
                    combinedStream.take>0 ? new BsonDocument("$limit", combinedStream.take.Value) : null ,

            }.Where(m => m != null).ToArray();

            return pipeline;

        }
    }
}
