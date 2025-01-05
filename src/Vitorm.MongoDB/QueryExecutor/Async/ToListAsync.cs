using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using MongoDB.Driver;

using Vit.Linq;

using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    public partial class ToListAsync : IQueryExecutor
    {
        public static readonly ToListAsync Instance = new();

        public string methodName => nameof(Queryable_Extensions.ToListAsync);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            // #1
            CombinedStream combinedStream = execArg.combinedStream;
            var dbContext = execArg.dbContext;


            IQueryable query = null;
            if (combinedStream.source is SourceStream sourceStream)
            {
                query = sourceStream.GetSource() as IQueryable;
            }
            else if (combinedStream.source is CombinedStream baseStream)
            {
                query = (baseStream.source as SourceStream)?.GetSource() as IQueryable;
            }

            var entityType = query.ElementType;
            var resultEntityType = execArg.expression.Type.GetGenericArguments().First().GetGenericArguments().First();

            return Execute_MethodInfo(entityType, resultEntityType).Invoke(null, new object[] { execArg });
        }


        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, Task<List<string>>>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);


        public static Task<List<ResultEntity>> Execute<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            return arg.dbContext.GetSearchExecutor(arg)?.ToListAsync<Entity, ResultEntity>(arg);
        }


    }

}