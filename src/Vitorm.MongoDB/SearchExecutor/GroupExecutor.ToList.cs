using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.Entity;
using Vitorm.MongoDB.QueryExecutor;

namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class GroupExecutor
    {

        public List<ResultEntity> ToList<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            Type keyType;
            var resultSelector = arg.combinedStream.select.resultSelector;
            if (resultSelector == null)
            {
                keyType = typeof(ResultEntity);
            }
            else
            {
                var groupType = resultSelector.Lambda_GetParamTypes()[0];
                keyType = groupType.GetGenericArguments()[0];
            }

            return (List<ResultEntity>)ToList_MethodInfo(typeof(Entity), typeof(ResultEntity), keyType).Invoke(null, new[] { arg });
        }


        private static MethodInfo ToList_MethodInfo_;
        static MethodInfo ToList_MethodInfo(Type entityType, Type resultEntityType, Type keyType) =>
            (ToList_MethodInfo_ ??= new Func<QueryExecutorArgument, List<string>>(ToList<string, string, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType, keyType);


        static List<ResultEntity> ToList<Entity, ResultEntity, Key>(QueryExecutorArgument arg)
        {
            var combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));


            using var cursor = Execute<Entity, ResultEntity>(arg, entityDescriptor);

            var lambdaExpression = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();
            var delSelect = (Func<IGrouping<Key, Entity>, ResultEntity>)lambdaExpression.Compile();

            var groups = ReadGroups<Key, Entity>(dbContext, entityDescriptor, cursor);

            return groups.Select(delSelect).ToList();

        }
        static IAsyncCursor<BsonDocument> Execute<Entity, ResultEntity>(QueryExecutorArgument arg, IEntityDescriptor entityDescriptor)
        {
            var dbContext = arg.dbContext;
            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>(entityDescriptor.tableName);

            var pipeline = GetPipeline(arg);

            // Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: pipeline.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = arg.combinedStream.method ?? "ToList",
                    ["combinedStream"] = arg.combinedStream,
                }))
            );

            if (arg.combinedStream.take == 0) return null;

            // Execute aggregation
            return dbContext.session == null ? collection.Aggregate<BsonDocument>(pipeline) : collection.Aggregate<BsonDocument>(dbContext.session, pipeline);
        }
        static IEnumerable<IGrouping<Key, Element>> ReadGroups<Key, Element>(DbContext dbContext, IEntityDescriptor entityDescriptor, IAsyncCursor<BsonDocument> cursor)
        {
            if (cursor == null) yield break;
            while (cursor.MoveNext())
            {
                foreach (BsonDocument document in cursor.Current)
                {
                    yield return new Grouping<Key, Element>(dbContext, entityDescriptor, document);
                }
            }
        }

    }
}
