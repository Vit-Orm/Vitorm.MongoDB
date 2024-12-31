using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.Entity;
using Vitorm.StreamQuery;


namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class PlainSearchExecutor : ISearchExecutor
    {
        protected virtual bool IsMatch<ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            CombinedStream combinedStream = arg.combinedStream;

            var dbContext = arg.dbContext;

            if (combinedStream.source is not SourceStream) return false;
            if (combinedStream.isGroupedStream) return false;
            if (combinedStream.joins?.Any() == true) return false;
            if (combinedStream.distinct != null) return false;

            return true;
        }


        #region Async

        public async Task<bool> ExecuteSearchAsync<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            if (!IsMatch(arg)) return false;

            #region getList
            if (arg.getList)
            {
                CombinedStream combinedStream = arg.combinedStream;
                var dbContext = arg.dbContext;
                var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));

                var fluent = ExecuteQuery<Entity, ResultEntity>(arg, entityDescriptor);


                List<ResultEntity> result;

                using var cursor = await fluent.ToCursorAsync();
                if (combinedStream.select?.resultSelector != null)
                {
                    // Select
                    var lambdaExp = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();

                    var delSelect = lambdaExp.Compile() as Func<Entity, ResultEntity>;

                    var entities = await ReadListAsync<Entity>(dbContext, entityDescriptor, cursor);
                    result = entities.Select(delSelect).ToList();
                }
                else
                {
                    result = await ReadListAsync<ResultEntity>(dbContext, entityDescriptor, cursor);
                }

                arg.list = result;
                return true;
            }
            #endregion

            return false;
        }

        static async Task<List<Entity>> ReadListAsync<Entity>(DbContext dbContext, IEntityDescriptor entityDescriptor, IAsyncCursor<BsonDocument> cursor)
        {
            var list = new List<Entity>();
            while (await cursor.MoveNextAsync())
            {
                IEnumerable<BsonDocument> batch = cursor.Current;
                foreach (BsonDocument document in batch)
                {
                    var entity = dbContext.Deserialize<Entity>(document, entityDescriptor);
                    list.Add(entity);
                }
            }
            return list;
        }
        #endregion



        #region Sync

        public bool ExecuteSearch<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            if (!IsMatch(arg)) return false;

            #region getList
            if (arg.getList)
            {
                CombinedStream combinedStream = arg.combinedStream;
                var dbContext = arg.dbContext;
                var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));

                var fluent = ExecuteQuery<Entity, ResultEntity>(arg, entityDescriptor);


                // #6 read entity
                List<ResultEntity> result;

                using var cursor = fluent.ToCursor();
                if (combinedStream.select?.resultSelector != null)
                {
                    // Select
                    var lambdaExp = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();

                    var delSelect = (Func<Entity, ResultEntity>)lambdaExp.Compile();
                    Type resultEntityType = typeof(ResultEntity);

                    var entities = ReadList<Entity>(dbContext, entityDescriptor, cursor);

                    result = entities.Select(delSelect).ToList();
                }
                else
                {
                    result = ReadList<ResultEntity>(dbContext, entityDescriptor, cursor).ToList();
                }

                arg.list = result;
                return true;
            }
            #endregion

            return false;
        }

        public static IEnumerable<Entity> ReadList<Entity>(DbContext dbContext, IEntityDescriptor entityDescriptor, IAsyncCursor<BsonDocument> cursor)
        {
            while (cursor.MoveNext())
            {
                IEnumerable<BsonDocument> batch = cursor.Current;
                foreach (BsonDocument document in batch)
                {
                    yield return dbContext.Deserialize<Entity>(document, entityDescriptor);
                }
            }
        }
        #endregion





        IFindFluent<BsonDocument, BsonDocument> ExecuteQuery<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg, IEntityDescriptor entityDescriptor)
        {
            // #1
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var translateService = dbContext.translateService;

            // #2 filter
            var filter = translateService.TranslateFilter(arg.execArg, combinedStream);

            // #3 execute query
            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>(entityDescriptor.tableName);
            var fluent = dbContext.session == null ? collection.Find(filter) : collection.Find(dbContext.session, filter);


            // #4 Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: filter.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = arg.execArg.combinedStream.method ?? "ToList",
                    ["combinedStream"] = combinedStream,
                }))
            );

            // #5 sortDoc
            BsonDocument sortDoc = null;
            if (combinedStream.orders?.Any() == true)
            {
                sortDoc = new BsonDocument();
                combinedStream.orders.ForEach(item =>
                {
                    var fieldPath = translateService.GetFieldPath(arg.execArg, item.member);
                    sortDoc.Add(fieldPath, BsonValue.Create(item.asc ? 1 : -1));
                });
            }
            if (sortDoc != null) fluent = fluent.Sort(sortDoc);

            // #6 skip take
            if (combinedStream.skip > 0) fluent = fluent.Skip(combinedStream.skip);
            if (combinedStream.take > 0) fluent = fluent.Limit(combinedStream.take);

            return fluent;
        }
    }
}
