using System.Threading.Tasks;

namespace Vitorm.MongoDB.SearchExecutor
{
    public interface ISearchExecutor
    {
        Task<bool> ExecuteSearchAsync<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg);

        bool ExecuteSearch<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg);
    }
}
