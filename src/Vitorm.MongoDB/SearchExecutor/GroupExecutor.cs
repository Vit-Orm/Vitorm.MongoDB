using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

using SharpCompress.Common;

using Vit.Linq.ExpressionNodes.ComponentModel;

using Vitorm.Entity;
using Vitorm.Entity.PropertyType;
using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.SearchExecutor
{
    public class GroupExecutor : ISearchExecutor
    {
        protected virtual bool IsMatch<ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            CombinedStream combinedStream = arg.combinedStream;

            var dbContext = arg.dbContext;

            //if (combinedStream.source is not SourceStream) return false;
            if (!combinedStream.isGroupedStream) return false;
            if (combinedStream.joins?.Any() == true) return false;
            if (combinedStream.distinct != null) return false;

            return true;
        }


        public async Task<bool> ExecuteSearchAsync<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            if (!IsMatch(arg)) return false;


            return false;
        }



        public bool ExecuteSearch<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            if (!IsMatch(arg)) return false;


            Type keyType;
            var resultSelector = arg.combinedStream.select.resultSelector;
            if (resultSelector == null)
            {
                keyType = typeof(ResultEntity);
            }
            else
            {
                var groupType = resultSelector.Lambda_GetParamTypes()[0];
                keyType = groupType.GetGenericArguments()[0];
            }

            return (bool)Execute_MethodInfo(typeof(Entity), typeof(ResultEntity), keyType).Invoke(null, new[] { arg });
        }

        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType, Type keyType) =>
            (Execute_MethodInfo_ ??= new Func<SearchExecutorArgument<string>, bool>(Execute<string, string, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType, keyType);

        static bool Execute<Entity, ResultEntity, Key>(SearchExecutorArgument<ResultEntity> arg)
        {
             
            if (arg.getList)
            {

                var combinedStream = arg.combinedStream;
                var dbContext = arg.dbContext;
                var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));

                if (combinedStream.select?.resultSelector == null)
                {
                    throw new NotImplementedException();
                }

                using var cursor = ExecuteQuery<Entity, ResultEntity>(arg, entityDescriptor);

                var lambdaExpression = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();
                var delSelect = (Func<IGrouping<Key, Entity>, ResultEntity>)lambdaExpression.Compile();

                var groups = ReadGroups<Key, Entity>(dbContext, entityDescriptor, cursor);

                arg.list = groups.Select(delSelect).ToList();
                return true;
            }


            return false;
        }

        static IEnumerable<IGrouping<Key, Element>> ReadGroups<Key, Element>(DbContext dbContext, IEntityDescriptor entityDescriptor, IAsyncCursor<BsonDocument> cursor)
        {
            while (cursor.MoveNext())
            {
                foreach (BsonDocument document in cursor.Current)
                {
                    yield return new Grouping<Key, Element>(dbContext, entityDescriptor, document);
                }
            }
        }
        class Grouping<TKey, TElement> : IGrouping<TKey, TElement>
        {
            class KeyWrap 
            {
                public TKey Key { get; set; }
            }
            public Grouping(DbContext dbContext, IEntityDescriptor entityDescriptor, BsonDocument document)
            {
                // #1 read key
                var docKey = document["key"];
                if (docKey.IsBsonDocument)
                {
                    Key = BsonSerializer.Deserialize<TKey>(docKey.AsBsonDocument);
                }
                else
                {
                    Key = BsonSerializer.Deserialize<KeyWrap>(new BsonDocument("Key", docKey)).Key;
                }

                // #2 read list
                list = document["items"]?.AsBsonArray.AsQueryable()?.Select(item => dbContext.Deserialize<TElement>(item.AsBsonDocument, entityDescriptor));
            }
            public TKey Key { get; private set; }

            IEnumerable<TElement> list;

            public IEnumerator<TElement> GetEnumerator() => list.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        static IAsyncCursor<BsonDocument> ExecuteQuery<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg, IEntityDescriptor entityDescriptor)
        {
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var translateService = dbContext.translateService;

            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>(entityDescriptor.tableName);


            // #2 filter
            var filter = combinedStream?.where == null ? null : translateService.TranslateFilter(arg.execArg, combinedStream.where);

            // #3 groupByFields
            List<(string field, string fieldAs)> groupFields = new();
            BsonValue groupByFields   ;
            BsonValue projectFields  ;
            var groupFieldArg = new QueryExecutorArgument_GroupFilter(arg.execArg, groupFields);
            #region groupByFields
            {
                var node = combinedStream.groupByFields;                 

                if (node?.nodeType == NodeType.New)
                {
                    ExpressionNode_New newNode = node;
                    newNode.constructorArgs.ForEach(nodeArg =>
                    {
                        var fieldAs = nodeArg.name;
                        var field = translateService.GetFieldPath(arg.execArg, nodeArg.value);
                        groupFields.Add((field, fieldAs));
                    });

                    groupByFields = new BsonDocument(groupFields.ToDictionary(field => field.fieldAs, field => "$" + field.field));
                    projectFields = new BsonDocument(groupFields.ToDictionary(field => field.fieldAs, field => "$_id." + field.fieldAs));
                }
                else if (node?.nodeType == NodeType.Member)
                {
                    string fieldAs = null;
                    var field = translateService.GetFieldPath(arg.execArg, node);
                    groupFields.Add((field, fieldAs));

                    groupByFields = "$" + field;
                    projectFields = "$_id";
                }
                else
                {
                    throw new NotSupportedException("[GroupExecutor] groupByFields is not valid: NodeType must be New or Member");
                }
            }
            #endregion

            // #4 filter to groups
            var groupFilter = combinedStream.having == null ? null : translateService.TranslateFilter(groupFieldArg, combinedStream.having);


            // #5 order by fields
            var orderFields = GetOrderFields<Entity, ResultEntity>(groupFieldArg);
            var orderByFields = orderFields == null ? null : new BsonDocument(orderFields.ToDictionary(field => field.field, field => BsonValue.Create(field.asc ? 1 : -1)));

            // Aggregation pipeline
            var pipeline = new[]
            {
                    //new BsonDocument("$match", new BsonDocument("userId", new BsonDocument("$gt", 1))),
                    filter == null ? null : new BsonDocument("$match", filter),

                    new BsonDocument("$group", new BsonDocument
                    {
                        //{ "_id", new BsonDocument { { "userFatherId", "$userFatherId" }, { "userMotherId", "$userMotherId" } } },
                        { "_id", groupByFields },
                        { "count", new BsonDocument("$sum", 1) },
                        { "items", new BsonDocument("$push", "$$ROOT") },
                    }),

                    //new BsonDocument("$match", new BsonDocument("count", new BsonDocument("$gte", 1))),
                    groupFilter == null ? null : new BsonDocument("$match", groupFilter),

                    //new BsonDocument("$sort", new BsonDocument("count", -1)),
                     orderByFields == null ? null : new BsonDocument("$sort", orderByFields),

                    new BsonDocument("$project", new BsonDocument
                    {
                        //{ "key",  new BsonDocument
                        //    {
                        //        { "userFatherId", "$_id.userFatherId" },
                        //        { "userMotherId", "$_id.userMotherId" },
                        //   }
                        //},
                        { "key", projectFields },
                        { "items", 1 },
                        { "count", 1 },
                        { "_id", 0 },
                    }),

                    //new BsonDocument("$skip", 1),
                    combinedStream.skip>0 ? new BsonDocument("$skip", combinedStream.skip.Value) : null ,

                    //new BsonDocument("$limit", 5),
                    combinedStream.take>0 ? new BsonDocument("$limit", combinedStream.take.Value) : null ,

            }.Where(m => m != null).ToArray();

            // Execute aggregation
            return dbContext.session == null ? collection.Aggregate<BsonDocument>(pipeline) : collection.Aggregate<BsonDocument>(dbContext.session, pipeline);
        }


         

        static List<(string field, bool asc, int index)> GetOrderFields<Entity, ResultEntity>(QueryExecutorArgument arg)
        {
            return arg.combinedStream.orders?.Select((orderField, index) =>
            {
                var field = arg.GetFieldPath( orderField.member);
                return (field, orderField.asc, index);
            }).ToList();
        }


        public class QueryExecutorArgument_GroupFilter : QueryExecutorArgument
        {
            List<(string field, string fieldAs)> groupFields;
            public QueryExecutorArgument_GroupFilter(QueryExecutorArgument arg, List<(string field, string fieldAs)> groupFields)
            {
                this.combinedStream = arg.combinedStream;
                this.dbContext = arg.dbContext;
                this.expression = arg.expression;
                this.expressionResultType = arg.expressionResultType;
                this.groupFields = groupFields;
            }

            public override string GetFieldPath(ExpressionNode member)
            {
                switch (member?.nodeType)
                {
                    case NodeType.MethodCall:
                        {
                            ExpressionNode_MethodCall methodCall = member;

                            switch (methodCall.methodName)
                            {
                                // ##1 Count
                                case nameof(Enumerable.Count) when methodCall.@object is null && methodCall.arguments.Length == 1:
                                    {
                                        return "count";
                                    }
                            }
                            break;
                        }
                }
                var field = base.GetFieldPath(member);
                var fieldAs = groupFields.First(f => f.field == field).fieldAs;
                if (fieldAs == null) return "_id";
                return "_id." + fieldAs;
            }
        }

      
    }
}
