using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using MongoDB.Driver;

using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    public partial class ToList : IQueryExecutor
    {
        public static readonly ToList Instance = new();

        public string methodName => nameof(Enumerable.ToList);



        public object ExecuteQuery(QueryExecutorArgument arg)
        {
            IQueryable query = null;
            if (arg.combinedStream.source is SourceStream sourceStream)
            {
                query = sourceStream.GetSource() as IQueryable;
            }
            else if (arg.combinedStream.source is CombinedStream baseStream)
            {
                query = (baseStream.source as SourceStream)?.GetSource() as IQueryable;
            }

            var entityType = query?.ElementType;
            var resultEntityType = arg.expression.Type.GetGenericArguments().First();

            return Execute_MethodInfo(entityType, resultEntityType).Invoke(null, new object[] { arg });
        }




        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, List<string>>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);



        public static List<ResultEntity> Execute<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            var executor = arg.dbContext.GetSearchExecutor(arg);
            if (executor == null) throw new NotImplementedException();
            return executor.ToList<Entity, ResultEntity>(arg);
        }








    }

}