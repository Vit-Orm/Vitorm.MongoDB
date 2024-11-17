using System;
using System.Linq.Expressions;

using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    public class QueryExecutorArgument : IDisposable
    {
        public CombinedStream combinedStream;
        public DbContext dbContext;

        public Expression expression;
        public Type expressionResultType;

        public Action dispose;

        public void Dispose()
        {
            dispose?.Invoke();
        }
    }

}
