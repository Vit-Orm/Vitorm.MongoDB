using System;
using System.Linq;
using System.Linq.Expressions;

using Vit.Linq.ExpressionNodes.ComponentModel;

using Vitorm.Entity.PropertyType;
using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    public class QueryExecutorArgument : IDisposable
    {
        public CombinedStream combinedStream;
        public DbContext dbContext;

        public Expression expression;
        public Type expressionResultType;

        public Action dispose;

        public void Dispose()
        {
            dispose?.Invoke();
        }
        public virtual string GetFieldPath(ExpressionNode member) => GetFieldPath(this.dbContext, member, out Type propertyType);

        public static string GetFieldPath(DbContext dbContext, ExpressionNode member, out Type propertyType)
        {
            switch (member?.nodeType)
            {
                case NodeType.Member:
                    {
                        if (member.objectValue != null)
                        {
                            // nested field
                            var parentPath = GetFieldPath(dbContext, member.objectValue, out Type parentPropertyType);

                            var memberType = member.objectValue.Member_GetType();
                            // bool?.Value
                            if (member.memberName == nameof(Nullable<bool>.Value) && TypeUtil.IsNullable(memberType))
                            {
                                propertyType = parentPropertyType;
                                return parentPath;
                            }


                            if (!string.IsNullOrWhiteSpace(member.memberName))
                            {
                                var parentEntityDescriptor = dbContext.GetEntityDescriptor(parentPropertyType);
                                var propertyDescriptor = parentEntityDescriptor?.properties?.FirstOrDefault(property => property.propertyName == member.memberName);

                                if (propertyDescriptor != null)
                                {
                                    propertyType = propertyDescriptor.propertyType.type;

                                    var columnName = propertyDescriptor.columnName;

                                    var fieldPath = columnName;
                                    if (parentPath != null) fieldPath = parentPath + "." + fieldPath;
                                    return fieldPath;
                                }
                            }
                            break;
                        }
                        else
                        {
                            // entity root
                            propertyType = member.Member_GetType();
                            if (string.IsNullOrWhiteSpace(member.memberName)) return null;
                            break;
                        }
                    }
                case NodeType.ArrayIndex:
                    {
                        ExpressionNode_ArrayIndex arrayIndex = member;

                        var index = arrayIndex.right.value;
                        var parentPath = GetFieldPath(dbContext, arrayIndex.left, out Type parentPropertyType);

                        var elementType = TypeUtil.GetElementTypeFromArray(parentPropertyType);
                        if (elementType != null)
                        {
                            propertyType = elementType;

                            var filePath = $"{index}";
                            if (parentPath != null) filePath = parentPath + "." + filePath;
                            return filePath;
                        }
                        break;
                    }

                case NodeType.MethodCall:
                    {
                        ExpressionNode_MethodCall methodCall = member;

                        switch (methodCall.methodName)
                        {
                            // ##1 List.get_Item
                            case "get_Item" when methodCall.@object is not null && methodCall.arguments.Length == 1:
                                {
                                    var index = methodCall.arguments[0]?.value;
                                    var parentPath = GetFieldPath(dbContext, methodCall.@object, out Type parentPropertyType);

                                    var elementType = TypeUtil.GetElementTypeFromArray(parentPropertyType);
                                    if (elementType != null)
                                    {
                                        propertyType = elementType;

                                        var filePath = $"{index}";
                                        if (parentPath != null) filePath = parentPath + "." + filePath;
                                        return filePath;
                                    }
                                    break;
                                }
                            // ##2 Count
                            case nameof(Enumerable.Count) when methodCall.@object is null && methodCall.arguments.Length == 1:
                                {
                                    propertyType = null;
                                    return "count";
                                }
                        }
                        break;
                    }
            }

            throw new InvalidOperationException($"Can not get fieldPath from member:{member?.nodeType}");
        }



        public static string GetFieldPath(DbContext dbContext, ExpressionNode member, out IPropertyType propertyType)
        {
            switch (member?.nodeType)
            {
                case NodeType.Member:
                    {
                        if (member.objectValue != null)
                        {
                            // nested field
                            var parentPath = GetFieldPath(dbContext, member.objectValue, out IPropertyType parentPropertyType);

                            var memberType = member.objectValue.Member_GetType();
                            // bool?.Value
                            if (member.memberName == nameof(Nullable<bool>.Value) && TypeUtil.IsNullable(memberType))
                            {
                                propertyType = parentPropertyType;
                                return parentPath;
                            }


                            if (!string.IsNullOrWhiteSpace(member.memberName) && parentPropertyType is IPropertyObjectType parentObjectType)
                            {
                                var propertyDescriptor = parentObjectType.properties?.FirstOrDefault(property => property.propertyName == member.memberName);

                                if (propertyDescriptor != null)
                                {
                                    propertyType = propertyDescriptor.propertyType;

                                    var columnName = propertyDescriptor.columnName;

                                    var fieldPath = columnName;
                                    if (parentPath != null) fieldPath = parentPath + "." + fieldPath;
                                    return fieldPath;
                                }
                            }
                            break;
                        }
                        else
                        {
                            // entity root
                            var entityType = member.Member_GetType();
                            var entityDescriptor = dbContext.GetEntityDescriptor(entityType);
                            propertyType = entityDescriptor.propertyType;

                            if (string.IsNullOrWhiteSpace(member.memberName)) return null;
                            break;
                        }
                    }
                case NodeType.ArrayIndex:
                    {
                        ExpressionNode_ArrayIndex arrayIndex = member;

                        var index = arrayIndex.right.value;
                        var parentPath = GetFieldPath(dbContext, arrayIndex.left, out IPropertyType parentPropertyType);

                        if (parentPropertyType is IPropertyArrayType arrayType)
                        {
                            propertyType = arrayType.elementPropertyType;

                            var filePath = $"{index}";
                            if (parentPath != null) filePath = parentPath + "." + filePath;
                            return filePath;
                        }
                        break;
                    }

                case NodeType.MethodCall:
                    {
                        ExpressionNode_MethodCall methodCall = member;

                        switch (methodCall.methodName)
                        {
                            // ##1 List.get_Item
                            case "get_Item" when methodCall.@object is not null && methodCall.arguments.Length == 1:
                                {
                                    var index = methodCall.arguments[0]?.value;
                                    var parentPath = GetFieldPath(dbContext, methodCall.@object, out IPropertyType parentPropertyType);

                                    if (parentPropertyType is IPropertyArrayType arrayType)
                                    {
                                        propertyType = arrayType?.elementPropertyType;

                                        var filePath = $"{index}";
                                        if (parentPath != null) filePath = parentPath + "." + filePath;
                                        return filePath;
                                    }
                                    break;
                                }
                            // ##2 Count
                            case nameof(Enumerable.Count) when methodCall.@object is null && methodCall.arguments.Length == 1:
                                {
                                    propertyType = null;
                                    return "count";
                                }
                        }
                        break;
                    }
            }

            throw new InvalidOperationException($"Can not get fieldPath from member:{member?.nodeType}");
        }
    }

}
