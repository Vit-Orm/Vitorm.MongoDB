using System;
using System.Linq;

using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    public partial class ToExecuteString : IQueryExecutor
    {
        public static readonly ToExecuteString Instance = new();

        public string methodName => nameof(Orm_Extensions.ToExecuteString);

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

            var executor = arg.dbContext.GetSearchExecutor(arg);
            if (executor == null) throw new NotImplementedException();
            return executor.ToExecuteString(arg, entityType);
        }

    }

}