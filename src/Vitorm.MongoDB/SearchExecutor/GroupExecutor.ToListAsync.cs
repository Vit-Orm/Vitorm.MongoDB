using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.Entity;
using Vitorm.MongoDB.QueryExecutor;

namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class GroupExecutor
    {

        public Task<List<ResultEntity>> ToListAsync<Entity, ResultEntity>(QueryExecutorArgument arg)
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

            return (Task<List<ResultEntity>>)ToListAsync_MethodInfo(typeof(Entity), typeof(ResultEntity), keyType).Invoke(null, new[] { arg });
        }


        private static MethodInfo ToListAsync_MethodInfo_;
        static MethodInfo ToListAsync_MethodInfo(Type entityType, Type resultEntityType, Type keyType) =>
            (ToListAsync_MethodInfo_ ??= new Func<QueryExecutorArgument, Task<List<string>>>(ToListAsync<string, string, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType, keyType);


        static async Task<List<ResultEntity>> ToListAsync<Entity, ResultEntity, Key>(QueryExecutorArgument arg)
        {
            var combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));


            using var cursor = await ExecuteAsync<Entity, ResultEntity>(arg, entityDescriptor);

            var lambdaExpression = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();
            var delSelect = (Func<IGrouping<Key, Entity>, ResultEntity>)lambdaExpression.Compile();

            var list = await ReadListAsync<Entity, ResultEntity, Key>(dbContext, entityDescriptor, cursor, delSelect);

            return list;

        }
        static Task<IAsyncCursor<BsonDocument>> ExecuteAsync<Entity, ResultEntity>(QueryExecutorArgument arg, IEntityDescriptor entityDescriptor)
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
                    ["Method"] = arg.combinedStream.method ?? "ToListAsync",
                    ["combinedStream"] = arg.combinedStream,
                }))
            );

            // Execute aggregation
            return dbContext.session == null ? collection.AggregateAsync<BsonDocument>(pipeline) : collection.AggregateAsync<BsonDocument>(dbContext.session, pipeline);
        }

        static async Task<List<ResultEntity>> ReadListAsync<Entity, ResultEntity, Key>(DbContext dbContext, IEntityDescriptor entityDescriptor, IAsyncCursor<BsonDocument> cursor, Func<IGrouping<Key, Entity>, ResultEntity> Select)
        {
            List<ResultEntity> list = new();
            while (await cursor.MoveNextAsync())
            {
                foreach (BsonDocument document in cursor.Current)
                {
                    var group = new Grouping<Key, Entity>(dbContext, entityDescriptor, document);
                    list.Add(Select(group));
                }
            }
            return list;
        }







    }
}
