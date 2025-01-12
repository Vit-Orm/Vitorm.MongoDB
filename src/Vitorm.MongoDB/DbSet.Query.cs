﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using Vit.Linq;
using Vit.Linq.ExpressionNodes.ComponentModel;

using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;

namespace Vitorm.MongoDB
{
    public partial class DbSet<Entity> : IDbSet<Entity>
    {

        public virtual IQueryable<Entity> Query()
        {
            return QueryableBuilder.Build<Entity>(QueryExecutor, DbContext.dbConfig.dbGroupName);
        }


        #region QueryExecutor

        public static Dictionary<string, IQueryExecutor> defaultQueryExecutors = CreateDefaultQueryExecutors();
        public static Dictionary<string, IQueryExecutor> CreateDefaultQueryExecutors()
        {
            Dictionary<string, IQueryExecutor> defaultQueryExecutors = new();

            #region AddDefaultQueryExecutor
            void AddDefaultQueryExecutor(IQueryExecutor queryExecutor, string methodName = null)
            {
                defaultQueryExecutors[methodName ?? queryExecutor.methodName] = queryExecutor;
            }
            #endregion


            #region Sync
            //// Orm_Extensions
            //AddDefaultQueryExecutor(ExecuteUpdate.Instance);
            //AddDefaultQueryExecutor(ExecuteDelete.Instance);
            AddDefaultQueryExecutor(ToExecuteString.Instance);

            // ToList
            AddDefaultQueryExecutor(ToList.Instance);
            // Count TotalCount
            AddDefaultQueryExecutor(Count.Instance);
            AddDefaultQueryExecutor(Count.Instance, methodName: nameof(Queryable_Extensions.TotalCount));

            //// ToListAndTotalCount
            //AddDefaultQueryExecutor(ToListAndTotalCount.Instance);

            // FirstOrDefault First LastOrDefault Last
            AddDefaultQueryExecutor(FirstOrDefault.Instance);
            AddDefaultQueryExecutor(FirstOrDefault.Instance, methodName: nameof(Queryable.First));
            AddDefaultQueryExecutor(FirstOrDefault.Instance, methodName: nameof(Queryable.LastOrDefault));
            AddDefaultQueryExecutor(FirstOrDefault.Instance, methodName: nameof(Queryable.Last));
            #endregion


            #region Async
            //// Orm_Extensions
            //AddDefaultQueryExecutor(ExecuteUpdateAsync.Instance);
            //AddDefaultQueryExecutor(ExecuteDeleteAsync.Instance);

            // ToList
            AddDefaultQueryExecutor(ToListAsync.Instance);
            //// Count TotalCount
            //AddDefaultQueryExecutor(CountAsync.Instance);
            //AddDefaultQueryExecutor(CountAsync.Instance, methodName: nameof(Queryable_AsyncExtensions.TotalCountAsync));

            //// ToListAndTotalCount
            //AddDefaultQueryExecutor(ToListAndTotalCountAsync.Instance);

            //// FirstOrDefault First LastOrDefault Last
            //AddDefaultQueryExecutor(FirstOrDefaultAsync.Instance);
            //AddDefaultQueryExecutor(FirstOrDefaultAsync.Instance, methodName: nameof(Queryable_AsyncExtensions.FirstAsync));
            //AddDefaultQueryExecutor(FirstOrDefaultAsync.Instance, methodName: nameof(Queryable_AsyncExtensions.LastOrDefaultAsync));
            //AddDefaultQueryExecutor(FirstOrDefaultAsync.Instance, methodName: nameof(Queryable_AsyncExtensions.LastAsync));
            #endregion

            return defaultQueryExecutors;
        }

        public Dictionary<string, IQueryExecutor> queryExecutors = defaultQueryExecutors;

        #endregion


        protected virtual bool QueryIsFromSameDb(object obj, Type elementType)
        {
            if (obj is not IQueryable query) return false;

            if (DbContext.dbConfig.dbGroupName == QueryableBuilder.GetQueryConfig(query) as string) return true;

            if (QueryableBuilder.BuildFrom(query))
                throw new InvalidOperationException("not allow query from different data source , queryable type: " + obj?.GetType().FullName);

            return false;
        }

        protected virtual object QueryExecutor(Expression expression, Type expressionResultType)
        {
            // #1 convert to ExpressionNode 
            ExpressionNode_Lambda node = DbContext.convertService.ConvertToData_LambdaNode(expression, autoReduce: true, isArgument: QueryIsFromSameDb);
            //var strNode = Json.Serialize(node);


            // #2 convert to Stream
            var stream = DbContext.streamReader.ReadFromNode(node);
            //var strStream = Json.Serialize(stream);

            if (stream is not CombinedStream combinedStream) combinedStream = new CombinedStream("tmp") { source = stream };

            var executorArg = new QueryExecutorArgument
            {
                combinedStream = combinedStream,
                dbContext = DbContext,
                expression = expression,
                expressionResultType = expressionResultType,
            };




            #region Execute by registered executor
            {
                var method = combinedStream.method;
                if (string.IsNullOrWhiteSpace(method)) method = nameof(Enumerable.ToList);
                if (queryExecutors.TryGetValue(method, out var queryExecutor))
                {
                    return queryExecutor.ExecuteQuery(executorArg);
                }
            }
            #endregion

            throw new NotSupportedException("not supported query method: " + combinedStream.method);
        }




    }
}
