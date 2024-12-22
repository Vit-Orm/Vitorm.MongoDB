using System.Linq;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.Entity;
using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.SearchExecutor
{
    public class GroupExecutor : ISearchExecutor
    {
        protected virtual bool IsMatch<ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            CombinedStream combinedStream = arg.combinedStream;

            var dbContext = arg.dbContext;

            //if (combinedStream.source is not SourceStream) return false;
            if (!combinedStream.isGroupedStream) return false;
            if (combinedStream.joins?.Any() == true) return false;
            if (combinedStream.distinct != null) return false;

            return true;
        }


        public async Task<bool> ExecuteSearchAsync<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            if (!IsMatch(arg)) return false;


            return false;
        }



        public bool ExecuteSearch<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            if (!IsMatch(arg)) return false;

            return false;
        }

        object PrepareQuery<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg, IEntityDescriptor entityDescriptor)
        {
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var translateService = dbContext.translateService;

            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>(entityDescriptor.tableName);


            // #2 filter
            var filter = translateService.TranslateFilter(arg.execArg, combinedStream);
            var fluent = dbContext.session == null ? collection.Find(filter) : collection.Find(dbContext.session, filter);

            var groupByFields = combinedStream.groupByFields;


            // Aggregation pipeline
            var pipeline = new[]
            {
                    new BsonDocument("$match", new BsonDocument("userId", new BsonDocument("$gt", 1))),
                    new BsonDocument("$group", new BsonDocument
                    {
                        { "_id", new BsonDocument { { "userFatherId", "$userFatherId" }, { "userMotherId", "$userMotherId" } } },
                        { "count", new BsonDocument("$sum", 1) }
                    }),
                    new BsonDocument("$match", new BsonDocument("count", new BsonDocument("$gte", 1))),
                    new BsonDocument("$sort", new BsonDocument("count", -1)),
                    new BsonDocument("$project", new BsonDocument
                    {
                        { "userFatherId", "$_id.userFatherId" },
                        { "userMotherId", "$_id.userMotherId" },
                        { "count", 1 },
                        { "_id", 0 }
                    })
            };


            // Execute the aggregation
            return collection.Aggregate<BsonDocument>(pipeline);
        }


    }
}
