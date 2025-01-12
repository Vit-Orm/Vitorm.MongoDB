using System;

using MongoDB.Bson;

using Vitorm.MongoDB.QueryExecutor;

namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class GroupExecutor
    {
        public string ToExecuteString(QueryExecutorArgument arg, Type entityType)
        {
            var dbContext = arg.dbContext;

            var pipeline = GetPipeline(arg);

            // Event_OnExecuting
            dbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: dbContext,
                executeString: pipeline.ToJson(),
                extraParam: new()
                {
                    ["entityDescriptor"] = dbContext.GetEntityDescriptor(entityType),
                    ["Method"] = arg.combinedStream.method ?? "ToExecuteString",
                    ["combinedStream"] = arg.combinedStream,
                }))
            );

            return pipeline.ToJson();
        }

    }
}
