﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using MongoDB.Driver;

using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    public partial class ToList : IQueryExecutor
    {
        public static readonly ToList Instance = new();

        public string methodName => nameof(Enumerable.ToList);



        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            IQueryable query = null;
            if (execArg.combinedStream.source is SourceStream sourceStream)
            {
                query = sourceStream.GetSource() as IQueryable;
            }
            else if (execArg.combinedStream.source is CombinedStream baseStream)
            {
                query = (baseStream.source as SourceStream)?.GetSource() as IQueryable;
            }

            var entityType = query?.ElementType;
            var resultEntityType = execArg.expression.Type.GetGenericArguments().First();

            return Execute_MethodInfo(entityType, resultEntityType).Invoke(null, new object[] { execArg });
        }




        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, List<string>>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);



        public static List<ResultEntity> Execute<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            return arg.dbContext.GetSearchExecutor(arg)?.ToList<Entity, ResultEntity>(arg);
        }








    }

}