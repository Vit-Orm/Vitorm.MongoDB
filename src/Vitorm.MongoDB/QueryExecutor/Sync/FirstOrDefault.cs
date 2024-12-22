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
    public partial class FirstOrDefault : IQueryExecutor
    {
        public static readonly FirstOrDefault Instance = new();

        public string methodName => nameof(Queryable.FirstOrDefault);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            using var _ = execArg;

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

            var entityType = query?.ElementType;
            var resultEntityType = execArg.expression.Type;

            return Execute_MethodInfo(entityType, resultEntityType).Invoke(null, new object[] { execArg });
        }


        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, string>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);


        public static ResultEntity Execute<Entity, ResultEntity>(QueryExecutorArgument execArg)
        {
            // #1
            CombinedStream combinedStream = execArg.combinedStream;
            var dbContext = execArg.dbContext;
            var translateService = dbContext.translateService;

            var entityType = typeof(Entity);
            var entityDescriptor = dbContext.GetEntityDescriptor(entityType);


            // #2 filter
            var filter = translateService.TranslateFilter(execArg, combinedStream);


            #region #3 sortDoc
            List<(string fieldPath, bool asc)> orders = combinedStream.orders?.Select(item =>
            {
                var fieldPath = translateService.GetFieldPath(execArg, item.member);
                return (fieldPath, item.asc);
            }).ToList() ?? new();

            BsonDocument sortDoc = null;
            if (combinedStream.method?.Contains("Last") == true)
            {
                if (combinedStream.skip.HasValue)
                {
                    combinedStream.skip = combinedStream.skip.Value + (combinedStream.take ?? 0) - 1;
                }
                else
                {
                    //ReverseOrder

                    // make sure orders exist
                    if (!orders.Any())
                    {
                        orders.Add((entityDescriptor.key.columnName, true));
                    }
                    // reverse order
                    orders = orders.Select(order => (order.fieldPath, !order.asc)).ToList();
                }
            }
            if (orders?.Any() == true)
            {
                sortDoc = new BsonDocument();
                orders.ForEach(item =>
                {
                    sortDoc.Add(item.fieldPath, BsonValue.Create(item.asc ? 1 : -1));
                });
            }
            #endregion

            // #4 Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: filter.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = combinedStream.method,
                    ["sortDoc"] = sortDoc,
                    ["combinedStream"] = combinedStream,
                }))
            );



            // #5 execute query
            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>(entityDescriptor.tableName);
            var fluent = collection.Find(filter);


            if (sortDoc != null) fluent = fluent.Sort(sortDoc);
            if (combinedStream.skip > 0) fluent = fluent.Skip(combinedStream.skip);
            //if (combinedStream.take > 0) 
            fluent = fluent.Limit(combinedStream.take ?? 1);


            // #6 read entity
            BsonDocument document;

            // result 
            var method = combinedStream.method;
            if (method.EndsWith("Async")) method = method.Substring(0, method.Length - "Async".Length);
            switch (method)
            {
                case nameof(Queryable.FirstOrDefault):
                case nameof(Queryable.LastOrDefault):
                    {
                        document = fluent.FirstOrDefault();
                        break;
                    }
                case nameof(Queryable.First):
                case nameof(Queryable.Last):
                    {
                        document = fluent.First();
                        break;
                    }

                default: throw new NotSupportedException("not supported query type: " + combinedStream.method);
            }

            // Convert
            if (combinedStream.select?.resultSelector != null)
            {
                // Select
                var lambdaExp = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();

                var delSelect = (Func<Entity, ResultEntity>)lambdaExp.Compile();

                var entity = ReadEntity<Entity>(dbContext, entityDescriptor, document);
                return delSelect(entity);
            }
            else
            {
                return ReadEntity<ResultEntity>(dbContext, entityDescriptor, document);
            }
        }


        static Entity ReadEntity<Entity>(DbContext dbContext, IEntityDescriptor entityDescriptor, BsonDocument document)
        {
            return dbContext.Deserialize<Entity>(document, entityDescriptor);
        }

    }
}
