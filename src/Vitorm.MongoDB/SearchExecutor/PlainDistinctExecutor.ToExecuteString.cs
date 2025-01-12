using System;

using MongoDB.Bson;

using Vitorm.MongoDB.QueryExecutor;

namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class PlainDistinctExecutor
    {
        public string ToExecuteString(QueryExecutorArgument arg, Type entityType)
        {
            var dbContext = arg.dbContext;

            var entityReader = new EntityReader.EntityReader();
            entityReader.Init(dbContext, entityType, arg.combinedStream.select.fields);

            var pipeline = GetPipeline(arg, entityReader);

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
