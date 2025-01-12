using System;
using System.Linq;
using System.Reflection;

using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    public partial class FirstOrDefault : IQueryExecutor
    {
        public static readonly FirstOrDefault Instance = new();

        public string methodName => nameof(Queryable.FirstOrDefault);

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
            var resultEntityType = arg.expression.Type;

            return Execute_MethodInfo(entityType, resultEntityType).Invoke(null, new object[] { arg });
        }


        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, string>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);



        public static ResultEntity Execute<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            var executor = arg.dbContext.GetSearchExecutor(arg);
            if (executor == null) throw new NotImplementedException();
            return executor.FirstOrDefault<Entity, ResultEntity>(arg);
        }



    }
}
