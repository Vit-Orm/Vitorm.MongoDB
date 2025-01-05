using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.Entity;
using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;


namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class PlainExecutor : ISearchExecutor
    {

        public async Task<List<ResultEntity>> ToListAsync<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));

            var fluent = ExecuteQuery<Entity, ResultEntity>(arg, entityDescriptor);


            List<ResultEntity> result;

            using var cursor = await fluent?.ToCursorAsync();
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

            return result;
        }

        static async Task<List<Entity>> ReadListAsync<Entity>(DbContext dbContext, IEntityDescriptor entityDescriptor, IAsyncCursor<BsonDocument> cursor)
        {
            var list = new List<Entity>();
            if (cursor == null) return list;

            while (await cursor.MoveNextAsync())
            {
                foreach (BsonDocument document in cursor.Current)
                {
                    var entity = dbContext.Deserialize<Entity>(document, entityDescriptor);
                    list.Add(entity);
                }
            }
            return list;
        }


    }
}
