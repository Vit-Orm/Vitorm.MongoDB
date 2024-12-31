﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

using Vit.Linq.ExpressionNodes.ComponentModel;

using Vitorm.Entity;
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

            #region getList
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


                if (combinedStream.groupByFields?.nodeType == NodeType.Member)
                {
                    var lambdaExpression = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();
                    var delSelect = (Func<IGrouping<Key, Entity>, ResultEntity>)lambdaExpression.Compile();

                }
                else
                {
                    var lambdaExpression = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();
                    var delSelect = (Func<IGrouping<Key, Entity>, ResultEntity>)lambdaExpression.Compile();

                    var groups = ReadGroups<Key, Entity>(dbContext, entityDescriptor, cursor);

                    arg.list = groups.Select(delSelect).ToList();
                    return true;
                }

            }
            #endregion

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
            public Grouping(DbContext dbContext, IEntityDescriptor entityDescriptor, BsonDocument document)
            {

                // #1 read key
                var docKey = document["key"].AsBsonDocument;
                Key = BsonSerializer.Deserialize<TKey>(docKey);


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
            var filter = translateService.TranslateFilter(arg.execArg, combinedStream);

            // #3 groupByFields
            var groupFields = GetGroupFields<Entity, ResultEntity>(arg);
            var groupByFields = new BsonDocument(groupFields.ToDictionary(field => field.fieldAs, field => "$" + field.field));
            var projectFields = new BsonDocument(groupFields.ToDictionary(field => field.fieldAs, field => "$_id." + field.fieldAs));

            // #4 filter to groups
            var groupFilter = translateService.TranslateFilter(new QueryExecutorArgument_GroupFilter(arg.execArg, groupFields), combinedStream.having);


            // #5 order by fields
            var orderFields = GetOrderFields<Entity, ResultEntity>(arg);
            var orderByFields = new BsonDocument(orderFields.ToDictionary(field => "_id." + groupFields.First(f => f.field == field.field).fieldAs, field => BsonValue.Create(field.asc ? 1 : -1)));

            // Aggregation pipeline
            var pipeline = new[]
            {
                    //new BsonDocument("$match", new BsonDocument("userId", new BsonDocument("$gt", 1))),
                    new BsonDocument("$match", filter),

                    new BsonDocument("$group", new BsonDocument
                    {
                        //{ "_id", new BsonDocument { { "userFatherId", "$userFatherId" }, { "userMotherId", "$userMotherId" } } },
                        { "_id", groupByFields },
                        { "count", new BsonDocument("$sum", 1) },
                        { "items", new BsonDocument("$push", "$$ROOT") },
                    }),

                    //new BsonDocument("$match", new BsonDocument("count", new BsonDocument("$gte", 1))),
                    new BsonDocument("$match", groupFilter),

                    //new BsonDocument("$sort", new BsonDocument("count", -1)),
                    new BsonDocument("$sort", orderByFields),

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
            };

            // Execute aggregation
            return dbContext.session == null ? collection.Aggregate<BsonDocument>(pipeline) : collection.Aggregate<BsonDocument>(dbContext.session, pipeline);
        }



        static List<(string field, string fieldAs)> GetGroupFields<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            var combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var translateService = dbContext.translateService;

            var node = combinedStream.groupByFields;
            List<(string field, string fieldAs)> fields = new();

            if (node?.nodeType == NodeType.New)
            {
                ExpressionNode_New newNode = node;
                newNode.constructorArgs.ForEach(nodeArg =>
                {
                    var fieldAs = nodeArg.name;
                    var field = translateService.GetFieldPath(arg.execArg, nodeArg.value);
                    fields.Add((field, fieldAs));
                });
            }
            else if (node?.nodeType == NodeType.Member)
            {
                var fieldAs = "Key";
                var field = translateService.GetFieldPath(arg.execArg, node);
                fields.Add((field, fieldAs));
            }
            else
            {
                throw new NotSupportedException("[GroupExecutor] groupByFields is not valid: NodeType must be New or Member");
            }
            return fields;
        }

        static List<(string field, bool asc, int index)> GetOrderFields<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            var combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var translateService = dbContext.translateService;

            return combinedStream.orders?.Select((orderField, index) =>
            {
                var field = translateService.GetFieldPath(arg.execArg, orderField.member);
                return (field, orderField.asc, index);
            }).ToList() ?? new();
        }


        public class QueryExecutorArgument_GroupFilter : QueryExecutorArgument
        {
            List<(string field, string fieldAs)> groupFields;
            public QueryExecutorArgument_GroupFilter(QueryExecutorArgument arg, List<(string field, string fieldAs)> groupFields)
            {
                this.dbContext = arg.dbContext;
                this.groupFields = groupFields;
            }

            public override string GetFieldPath(ExpressionNode member)
            {
                var field = base.GetFieldPath(member);
                return "_id" + groupFields.First(f => f.field == field).fieldAs;
            }
        }

        /*
         
            // Aggregation pipeline
            var pipeline = new[]
            {
                    new BsonDocument("$match", new BsonDocument("userId", new BsonDocument("$gt", 1))),
                    new BsonDocument("$group", new BsonDocument
                    {
                        { "_id", new BsonDocument { { "userFatherId", "$userFatherId" }, { "userMotherId", "$userMotherId" } } },
                        { "count", new BsonDocument("$sum", 1) }
                    }),
                    new BsonDocument("$match", new BsonDocument("count", new BsonDocument("$gte", 1))),
                    new BsonDocument("$sort", new BsonDocument("count", -1)),
                    new BsonDocument("$project", new BsonDocument
                    {
                        { "userFatherId", "$_id.userFatherId" },
                        { "userMotherId", "$_id.userMotherId" },
                        { "count", 1 },
                        { "_id", 0 }
                    })
            };
         
         
         */
    }
}