using System.Collections.Generic;

using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.SearchExecutor
{
    public class SearchExecutorArgument<ResultEntity>
    {
        public QueryExecutorArgument execArg;
        public CombinedStream combinedStream => execArg.combinedStream;
        public DbContext dbContext => execArg.dbContext;

        public bool getList;
        public bool getTotalCount;


        public List<ResultEntity> list;
        public int? totalCount;
        public object extraResult;
    }

}
