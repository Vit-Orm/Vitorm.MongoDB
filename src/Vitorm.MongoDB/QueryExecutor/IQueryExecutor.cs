namespace Vitorm.MongoDB.QueryExecutor
{
    public interface IQueryExecutor
    {
        public string methodName { get; }
        object ExecuteQuery(QueryExecutorArgument execArg);
    }
}
