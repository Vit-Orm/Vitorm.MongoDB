
using FuncQueryExecutor = System.Func<Vitorm.MongoDB.QueryExecutor.QueryExecutorArgument, object>;

namespace Vitorm.MongoDB.QueryExecutor
{
    public class QueryExecutor_Delegate : IQueryExecutor
    {
        public QueryExecutor_Delegate(string methodName, FuncQueryExecutor queryExecutor)
        {
            this.methodName = methodName;
            this.queryExecutor = queryExecutor;
        }
        public string methodName { get; set; }
        FuncQueryExecutor queryExecutor;
        public object ExecuteQuery(QueryExecutorArgument execArg) => queryExecutor(execArg);
    }
}
