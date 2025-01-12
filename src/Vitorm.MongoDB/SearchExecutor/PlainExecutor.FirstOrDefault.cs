using System;
using System.Linq;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class PlainExecutor
    {
        public ResultEntity FirstOrDefault<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));


            if (combinedStream.method.Contains("Last"))
            {
                if (combinedStream.skip.HasValue)
                {
                    combinedStream.skip = combinedStream.skip.Value + (combinedStream.take ?? 0) - 1;
                }
                else
                {
                    arg.ReverseOrder();
                }
            }
            if (combinedStream.take != 0)
                combinedStream.take = 1;


            var fluent = ExecuteQuery<Entity, ResultEntity>(arg, entityDescriptor);

            var nullable = combinedStream.method.Contains("OrDefault");
            if (combinedStream.take == 0)
            {
                return nullable ? default : throw new InvalidOperationException("Sequence contains no elements");
            }


            // read entity
            BsonDocument document = nullable ? fluent?.FirstOrDefault() : fluent.First();

            if (combinedStream.select?.resultSelector != null)
            {
                // Select
                var lambdaExp = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();
                var delSelect = (Func<Entity, ResultEntity>)lambdaExp.Compile();

                var entity = dbContext.Deserialize<Entity>(document, entityDescriptor);
                return entity == null ? default : delSelect(entity);
            }
            else
            {
                var entity = dbContext.Deserialize<ResultEntity>(document, entityDescriptor);
                return entity;
            }

        }




    }
}
