using System;
using System.Collections.Generic;
using System.Linq;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.Entity;
using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;


namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class PlainSearchExecutor : ISearchExecutor
    {

        public List<ResultEntity> ToList<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));

            var fluent = ExecuteQuery<Entity, ResultEntity>(arg, entityDescriptor);


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

            return result;
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

    }
}
