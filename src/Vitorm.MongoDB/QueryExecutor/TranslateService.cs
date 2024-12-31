using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

using MongoDB.Bson;

using Vit.Linq.ExpressionNodes.ComponentModel;

using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    public class TranslateService
    {
        public virtual BsonDocument TranslateFilter(QueryExecutorArgument arg, CombinedStream combinedStream)
        {
            if (combinedStream?.where == null) return new();

            return TranslateFilter(arg, combinedStream.where);
        }



        public virtual string GetFieldPath(QueryExecutorArgument arg, ExpressionNode member)
        {
            return arg.GetFieldPath(member);
        }


        public virtual bool TryReadValue(QueryExecutorArgument arg, ExpressionNode node, out BsonValue value)
        {
            switch (node?.nodeType)
            {
                case NodeType.Constant:
                    {
                        ExpressionNode_Constant constant = node;

                        var v = constant.value;
                        if (v == null)
                        {
                            value = BsonValue.Create(null);
                            return true;
                        }

                        var type = constant.Constant_GetType();
                        if (TypeUtil.IsValueType(type))
                        {
                            value = BsonValue.Create(v);
                            return true;
                        }

                        if (TypeUtil.IsArrayType(type))
                        {
                            if (v is IEnumerable enumerable)
                            {
                                var array = new BsonArray();
                                foreach (var item in enumerable)
                                {
                                    array.Add(BsonValue.Create(item));
                                }
                                value = array;
                                return true;
                            }
                        }

                        value = null;
                        return false;
                    }

                case NodeType.MethodCall:
                    {
                        ExpressionNode_MethodCall methodCall = node;

                        //switch (methodCall.methodName)
                        //{ 
                        //    // ##4 String.Format(format: "{0}_{1}_{2}", "0", "1", "2")
                        //    case nameof(String.Format):
                        //        {
                        //            // convert to ExpressionNode.Add

                        //            // "{0}_{1}_{2}"
                        //            var format = methodCall.arguments[0].value as string;
                        //            var args = methodCall.arguments.AsQueryable().Skip(1).ToArray();

                        //            var nodeParts = SplitToNodeParts(format, args);

                        //            ExpressionNode nodeForAdd = null;
                        //            foreach (var child in nodeParts)
                        //            {
                        //                if (nodeForAdd == null) nodeForAdd = child;
                        //                else nodeForAdd = ExpressionNode.Add(left: nodeForAdd, right: child, typeof(string));
                        //            }

                        //            return $"({EvalExpression(arg, nodeForAdd)})";


                        //            static IEnumerable<ExpressionNode> SplitToNodeParts(string format, ExpressionNode[] args)
                        //            {
                        //                string pattern = @"(\{\d+\})|([^{}]+)";
                        //                var matches = Regex.Matches(format, pattern);

                        //                foreach (Match match in matches)
                        //                {
                        //                    var str = match.Value;
                        //                    if (str.StartsWith("{") && str.EndsWith("}"))
                        //                    {
                        //                        var argIndex = int.Parse(str.Substring(1, str.Length - 2));
                        //                        yield return args[argIndex];
                        //                    }
                        //                    else
                        //                    {
                        //                        yield return ExpressionNode.Constant(str, typeof(string));
                        //                    }
                        //                }
                        //            }
                        //        }

                        //}
                        break;
                    }

            }
            value = null;
            return false;
        }

        public virtual BsonDocument TranslateFilter(QueryExecutorArgument arg, ExpressionNode node)
        {
            switch (node?.nodeType)
            {
                case NodeType.AndAlso:
                    {
                        ExpressionNode_Binary binary = node;
                        return new BsonDocument("$and", new BsonArray { TranslateFilter(arg, binary.left), TranslateFilter(arg, binary.right) });
                    }
                case NodeType.OrElse:
                    {
                        ExpressionNode_Binary binary = node;
                        return new BsonDocument("$or", new BsonArray { TranslateFilter(arg, binary.left), TranslateFilter(arg, binary.right) });
                    }
                case NodeType.Not:
                    {
                        ExpressionNode_Not not = node;
                        //return new BsonDocument("$not", EvalExpression(arg, not.body));
                        return new BsonDocument("$nor", new BsonArray { TranslateFilter(arg, not.body) });
                    }
                case NodeType.Equal:
                case NodeType.NotEqual:
                case NodeType.LessThan:
                case NodeType.LessThanOrEqual:
                case NodeType.GreaterThan:
                case NodeType.GreaterThanOrEqual:
                    {
                        ExpressionNode_Binary binary = node;

                        string fieldPath;
                        BsonValue value;
                        bool fieldInLeft = true;
                        string operate;

                        if (TryReadValue(arg, binary.right, out value))
                        {
                            fieldPath = GetFieldPath(arg, binary.left);
                        }
                        else if (TryReadValue(arg, binary.left, out value))
                        {
                            fieldInLeft = false;
                            fieldPath = GetFieldPath(arg, binary.right);
                        }
                        else break;

                        operate = node.nodeType switch
                        {
                            NodeType.Equal => "$eq",
                            NodeType.NotEqual => "$ne",
                            NodeType.LessThan => fieldInLeft ? "$lt" : "$gt",
                            NodeType.LessThanOrEqual => fieldInLeft ? "$lte" : "$gte",
                            NodeType.GreaterThan => fieldInLeft ? "$gt" : "$lt",
                            NodeType.GreaterThanOrEqual => fieldInLeft ? "$gte" : "$lte",
                        };

                        return new BsonDocument(fieldPath, new BsonDocument(operate, value));
                    }
                case NodeType.Member:
                    {
                        var propertyType = node.Member_GetType();
                        var fieldPath = GetFieldPath(arg, node);
                        if (propertyType == typeof(bool) || propertyType == typeof(bool?))
                            return new BsonDocument(fieldPath, new BsonDocument("$eq", BsonValue.Create(true)));
                        break;
                    }
                case NodeType.MethodCall:
                    {
                        ExpressionNode_MethodCall methodCall = node;

                        switch (methodCall.methodName)
                        {

                            #region String method:  StartsWith EndsWith Contains
                            case nameof(string.StartsWith): // String.StartsWith
                                {
                                    var member = methodCall.@object;
                                    var value = methodCall.arguments[0];

                                    var fieldPath = GetFieldPath(arg, member);
                                    TryReadValue(arg, value, out var bValue);
                                    string strValue = bValue.AsString;
                                    strValue = Regex.Escape(strValue);

                                    var regex = $"^{strValue}";
                                    return new BsonDocument(fieldPath, new BsonDocument("$regex", BsonValue.Create(regex)));
                                }
                            case nameof(string.EndsWith): // String.EndsWith
                                {
                                    var member = methodCall.@object;
                                    var value = methodCall.arguments[0];

                                    var fieldPath = GetFieldPath(arg, member);
                                    TryReadValue(arg, value, out var bValue);
                                    string strValue = bValue.AsString;
                                    strValue = Regex.Escape(strValue);

                                    var regex = $"{strValue}$";
                                    return new BsonDocument(fieldPath, new BsonDocument("$regex", BsonValue.Create(regex)));
                                }
                            case nameof(string.Contains) when methodCall.methodCall_typeName == "String": // String.Contains
                                {
                                    var member = methodCall.@object;
                                    var value = methodCall.arguments[0];

                                    var fieldPath = GetFieldPath(arg, member);
                                    TryReadValue(arg, value, out var bValue);
                                    string strValue = bValue.AsString;
                                    strValue = Regex.Escape(strValue);

                                    var regex = $"{strValue}";
                                    return new BsonDocument(fieldPath, new BsonDocument("$regex", BsonValue.Create(regex)));
                                }
                            #endregion



                            // in
                            case nameof(List<string>.Contains) when methodCall.@object is not null && methodCall.arguments.Length == 1:
                                {
                                    var values = methodCall.@object;
                                    var member = methodCall.arguments[0];

                                    var fieldPath = GetFieldPath(arg, member);
                                    if (!TryReadValue(arg, values, out var array)) break;

                                    return new BsonDocument(fieldPath, new BsonDocument("$in", array));
                                }
                            case nameof(Enumerable.Contains) when methodCall.arguments.Length == 2:
                                {
                                    var values = methodCall.arguments[0];
                                    var member = methodCall.arguments[1];

                                    var fieldPath = GetFieldPath(arg, member);
                                    if (!TryReadValue(arg, values, out var array)) break;

                                    return new BsonDocument(fieldPath, new BsonDocument("$in", array));
                                }
                        }
                        throw new NotSupportedException("[TranslateService] not supported MethodCall: " + methodCall.methodName);
                    }
            }

            throw new NotSupportedException("[TranslateService] not supported notType: " + node?.nodeType);
        }

    }
}
