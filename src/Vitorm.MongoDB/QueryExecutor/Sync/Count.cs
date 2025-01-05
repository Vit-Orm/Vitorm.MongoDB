using System;
using System.Linq;

using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    /// <summary>
    /// Queryable.Count or Queryable_Extensions.TotalCount
    /// </summary>
    public partial class Count : IQueryExecutor
    {
        public static readonly Count Instance = new();

        public string methodName => nameof(Queryable.Count);

        public object ExecuteQuery(QueryExecutorArgument arg)
        {
            using var _ = arg;

            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            IQueryable query = null;
            if (combinedStream.source is SourceStream sourceStream)
            {
                query = sourceStream.GetSource() as IQueryable;
            }
            else if (combinedStream.source is CombinedStream baseStream)
            {
                query = (baseStream.source as SourceStream)?.GetSource() as IQueryable;
            }

            var skipAndTake = (combinedStream.skip, combinedStream.take);
            (combinedStream.skip, combinedStream.take) = (null, null);

            var entityType = query?.ElementType;

            var executor = arg.dbContext.GetSearchExecutor(arg);
            if (executor == null) throw new NotImplementedException();
            var count = executor.Count(arg, entityType);
            (combinedStream.skip, combinedStream.take) = skipAndTake;

            // Count and TotalCount
            if (count > 0 && combinedStream.method == nameof(Queryable.Count))
            {
                if (combinedStream.skip > 0) count = Math.Max(count - combinedStream.skip.Value, 0);

                if (combinedStream.take.HasValue)
                    count = Math.Min(count, combinedStream.take.Value);
            }

            return count;
        }





    }
}
