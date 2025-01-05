using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using MongoDB.Bson;

using Vit.Linq.ExpressionNodes;
using Vit.Linq.ExpressionNodes.ComponentModel;

namespace Vitorm.MongoDB.EntityReader
{
    /// <summary>
    ///  get all sql column values, compile EntityGenerator to Lambda .  Invoke the lambda when reading rows , pass sql column values as lambda args. 
    /// </summary>
    public class EntityReader
    {
        public List<IArgReader> entityArgReaders = new List<IArgReader>();
        protected Delegate lambdaCreateEntity;

        public void Init(DbContext dbContext, Type entityType, ExpressionNode resultSelector)
        {
            var cloner = new ExpressionNodeCloner();
            cloner.clone = (node) =>
            {
                if (node?.nodeType == NodeType.Member)
                {
                    ExpressionNode_Member member = node;

                    var argName = GetArgument(dbContext, member);

                    if (argName != null)
                    {
                        return (true, ExpressionNode.Member(parameterName: argName, memberName: null));
                    }
                }
                else if (node?.nodeType == NodeType.MethodCall)
                {
                    ExpressionNode_MethodCall methodCall = node;

                    //// deal with aggregate functions like Sum(id)
                    //if (methodCall.methodCall_typeName == "Enumerable")
                    //{
                    //    string argName = null;

                    //    var sqlColumnSentence = sqlTranslateService.EvalExpression(arg, node);
                    //    var columnType = methodCall.MethodCall_GetReturnType();
                    //    argName = GetArgument(config, sqlColumnSentence, columnType);
                    //    if (argName != null)
                    //    {
                    //        return (true, ExpressionNode.Member(parameterName: argName, memberName: null));
                    //    }
                    //}
                    throw new InvalidOperationException();
                }
                return default;
            };
            ExpressionNode newResultSelector = cloner.Clone(resultSelector);

            // compile ResultCreate lambda
            lambdaCreateEntity = CompileExpression(dbContext.convertService, entityArgReaders.Select(m => m.argName).ToArray(), newResultSelector);
        }

        public object ReadEntity(BsonDocument reader)
        {
            var lambdaArgs = entityArgReaders.Select(m => m.Read(reader)).ToArray();
            var entity = lambdaCreateEntity.DynamicInvoke(lambdaArgs);
            return entity;
        }



        protected string GetArgument(DbContext dbContext, ExpressionNode_Member member)
        {
            var fieldPath = dbContext.translateService.GetFieldPath(new() { dbContext = dbContext }, (ExpressionNode)member);

            IArgReader argReader = entityArgReaders.FirstOrDefault(reader => reader.fieldPath == fieldPath);

            if (argReader == null)
            {
                var argName = "arg_" + entityArgReaders.Count;

                var argType = member.Member_GetType();

                bool isValueType = TypeUtil.IsValueType(argType);
                if (isValueType)
                {
                    // Value arg 
                    argReader = new ValueReader(argType, fieldPath, argName);
                }
                else
                {
                    // Entity arg
                    //var entityDescriptor = config.queryTranslateArgument.dbContext.GetEntityDescriptor(argType);

                    //argReader = new ModelReader(config.sqlColumns, config.sqlTranslateService, tableName, argUniqueKey, argName, entityDescriptor);
                }
                entityArgReaders.Add(argReader);
            }
            return argReader.argName;
        }


        Delegate CompileExpression(ExpressionConvertService convertService, string[] parameterNames, ExpressionNode newExp)
        {
            var lambdaNode = ExpressionNode.Lambda(entityArgReaders.Select(m => m.argName).ToArray(), newExp);
            // var strNode = Json.Serialize(lambdaNode);

            var lambdaExp = convertService.ConvertToCode_LambdaExpression(lambdaNode, entityArgReaders.Select(m => m.entityType).ToArray());

            return lambdaExp.Compile();
        }

    }
}
