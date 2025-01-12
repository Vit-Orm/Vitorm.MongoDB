using System;
using System.Linq;
using System.Reflection;

using MongoDB.Bson;

using MongoDB.Driver;

using Vitorm.MongoDB.QueryExecutor;

namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class GroupExecutor
    {
        public ResultEntity FirstOrDefault<Entity, ResultEntity>(QueryExecutorArgument arg)
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

            return (ResultEntity)FirstOrDefault_MethodInfo(typeof(Entity), typeof(ResultEntity), keyType).Invoke(null, new[] { arg });
        }


        private static MethodInfo FirstOrDefault_MethodInfo_;
        static MethodInfo FirstOrDefault_MethodInfo(Type entityType, Type resultEntityType, Type keyType) =>
            (FirstOrDefault_MethodInfo_ ??= new Func<QueryExecutorArgument, string>(FirstOrDefault<string, string, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType, keyType);


        static ResultEntity FirstOrDefault<Entity, ResultEntity, Key>(QueryExecutorArgument arg)
        {
            var combinedStream = arg.combinedStream;
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


            using var cursor = Execute<Entity, ResultEntity>(arg, entityDescriptor);

            var nullable = combinedStream.method.Contains("OrDefault");
            if (combinedStream.take == 0)
            {
                return nullable ? default : throw new InvalidOperationException("Sequence contains no elements");
            }

            BsonDocument document = nullable ? cursor?.FirstOrDefault() : cursor.First();
            if (document == null) return default;

            var firstGroup = new Grouping<Key, Entity>(dbContext, entityDescriptor, document);

            var lambdaExpression = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();
            var delSelect = (Func<IGrouping<Key, Entity>, ResultEntity>)lambdaExpression.Compile();

            return delSelect(firstGroup);
        }




    }
}
