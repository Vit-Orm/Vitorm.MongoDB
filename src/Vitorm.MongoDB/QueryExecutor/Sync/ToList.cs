using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.Entity;
using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    public partial class ToList : IQueryExecutor
    {
        public static readonly ToList Instance = new();

        public string methodName => nameof(Enumerable.ToList);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            using var _ = execArg;

            var resultEntityType = execArg.expression.Type.GetGenericArguments()?.FirstOrDefault();
            return Execute(execArg, resultEntityType);
        }


        public static object Execute(QueryExecutorArgument execArg, Type resultEntityType)
        {
            CombinedStream combinedStream = execArg.combinedStream;
            var dbContext = execArg.dbContext;
            var translateService = dbContext.translateService;

            // #2 filter
            var filter = translateService.TranslateQuery(execArg, combinedStream);

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

            // #3 execute query
            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>(entityDescriptor.tableName);

            // #4 read entity
            object result;



            // Sort
            BsonDocument sortDoc = null;
            var fluent = collection.Find(filter);
            if (combinedStream.orders?.Any() == true)
            {
                sortDoc = new BsonDocument();

                combinedStream.orders.ForEach(item =>
                {
                    var fieldPath = translateService.GetFieldPath(execArg, item.member);
                    sortDoc.Add(fieldPath, BsonValue.Create(item.asc ? 1 : -1));
                });
                fluent = fluent.Sort(sortDoc);
            }

            if (combinedStream.skip > 0) fluent = fluent.Skip(combinedStream.skip);
            if (combinedStream.take > 0) fluent = fluent.Limit(combinedStream.take);

            using var cursor = fluent.ToCursor();

            // Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: filter.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "ToList",
                    ["sortDoc"] = sortDoc,
                    ["combinedStream"] = combinedStream,
                }))
            );

            if (combinedStream.select?.resultSelector != null)
            {
                // Select
                var lambdaExp = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();

                var delSelect = lambdaExp.Compile();

                result = Method_ReadListAndConvert.MakeGenericMethod(queryEntityType, resultEntityType)
                    .Invoke(null, new object[] { dbContext, entityDescriptor, cursor, delSelect });
            }
            else
            {
                result = Method_ReadList.MakeGenericMethod(queryEntityType)
                  .Invoke(null, new object[] { dbContext, entityDescriptor, cursor });
            }

            return result;
        }


        static MethodInfo Method_ReadListAndConvert = new Func<DbContext, IEntityDescriptor, IAsyncCursor<BsonDocument>, Func<object, object>, List<object>>(ReadListAndConvert<object, object>)
                    .Method.GetGenericMethodDefinition();

        static List<ResultEntity> ReadListAndConvert<Entity, ResultEntity>(DbContext dbContext, IEntityDescriptor entityDescriptor, IAsyncCursor<BsonDocument> cursor, Func<Entity, ResultEntity> delSelect)
        {
            var entities = Read<Entity>(dbContext, entityDescriptor, cursor);

            return entities.Select(delSelect).ToList();
        }



        static MethodInfo Method_ReadList = new Func<DbContext, IEntityDescriptor, IAsyncCursor<BsonDocument>, List<object>>(ReadList<object>)
                  .Method.GetGenericMethodDefinition();

        static List<Entity> ReadList<Entity>(DbContext dbContext, IEntityDescriptor entityDescriptor, IAsyncCursor<BsonDocument> cursor)
        {
            return Read<Entity>(dbContext, entityDescriptor, cursor).ToList();
        }


        static IEnumerable<Entity> Read<Entity>(DbContext dbContext, IEntityDescriptor entityDescriptor, IAsyncCursor<BsonDocument> cursor)
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
    }

}