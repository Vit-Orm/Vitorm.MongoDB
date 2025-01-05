using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

using Vit.Linq.ExpressionNodes.ComponentModel;

using Vitorm.Entity;
using Vitorm.MongoDB.QueryExecutor;
using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.SearchExecutor
{
    public partial class GroupExecutor : ISearchExecutor
    {
        public virtual bool IsMatch(QueryExecutorArgument arg)
        {
            CombinedStream combinedStream = arg.combinedStream;

            var dbContext = arg.dbContext;

            //if (combinedStream.source is not SourceStream) return false;
            if (!combinedStream.isGroupedStream) return false;
            if (combinedStream.joins?.Any() == true) return false;
            //if (combinedStream.distinct != null) return false;
            if (combinedStream.select?.resultSelector == null) return false;

            return true;
        }



        public static BsonDocument[] GetPipeline(QueryExecutorArgument arg)
        {
            CombinedStream combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var translateService = dbContext.translateService;

            // #2 filter
            var filter = combinedStream?.where == null ? null : translateService.TranslateFilter(arg, combinedStream.where);

            // #3 groupByFields
            List<(string field, string fieldAs)> groupFields = new();
            BsonValue groupByFields;
            var groupFieldArg = new QueryExecutorArgument_GroupFilter(arg, groupFields);

            #region groupByFields
            {
                var node = combinedStream.groupByFields;

                if (node?.nodeType == NodeType.New)
                {
                    ExpressionNode_New newNode = node;
                    newNode.constructorArgs.ForEach(nodeArg =>
                    {
                        var fieldAs = nodeArg.name;
                        var field = arg.GetFieldPath(nodeArg.value);
                        groupFields.Add((field, fieldAs));
                    });

                    groupByFields = new BsonDocument(groupFields.ToDictionary(field => field.fieldAs, field => "$" + field.field));
                }
                else if (node?.nodeType == NodeType.Member)
                {
                    string fieldAs = null;
                    var field = arg.GetFieldPath(node);
                    groupFields.Add((field, fieldAs));

                    groupByFields = "$" + field;
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
            List<(string field, bool asc, int index)> orderFields = combinedStream.orders?.Select((orderField, index) =>
            {
                var field = groupFieldArg.GetFieldPath(orderField.member);
                return (field, orderField.asc, index);
            }).ToList();
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
                        { "_id", 1 },
                        { "items", 1 },
                        { "count", 1 },
                    }),

                    //new BsonDocument("$skip", 1),
                    combinedStream.skip>0 ? new BsonDocument("$skip", combinedStream.skip.Value) : null ,

                    //new BsonDocument("$limit", 5),
                    combinedStream.take>0 ? new BsonDocument("$limit", combinedStream.take.Value) : null ,

            }.Where(m => m != null).ToArray();

            return pipeline;
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
                var docKey = document["_id"];
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

                var groupField = groupFields.FirstOrDefault(f => f.field == field);
                if (groupField.field == field)
                {
                    var fieldAs = groupField.fieldAs;
                    if (fieldAs == null) return "_id";
                    return "_id." + fieldAs;
                }

                // $ROOT
                groupField = groupFields.FirstOrDefault(f => f.field == "$ROOT");
                if (groupField == default) throw new InvalidOperationException();

                {
                    var fieldAs = groupField.fieldAs;
                    if (fieldAs == null) return "_id." + field;
                    return "_id." + fieldAs + "." + field;
                }
            }
        }


    }
}
