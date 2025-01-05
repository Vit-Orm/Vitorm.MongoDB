using System.Collections.Generic;
using System.Threading.Tasks;

using Vitorm.MongoDB.QueryExecutor;

namespace Vitorm.MongoDB.SearchExecutor
{
    public interface ISearchExecutor
    {
        bool IsMatch(QueryExecutorArgument arg);
        List<ResultEntity> ToList<Entity, ResultEntity>(QueryExecutorArgument arg);

        Task<List<ResultEntity>> ToListAsync<Entity, ResultEntity>(QueryExecutorArgument arg);



    }
}
