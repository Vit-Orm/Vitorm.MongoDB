using System.Linq;

using Vit.Linq.ExpressionNodes.ComponentModel;

using Vitorm.StreamQuery;

namespace Vitorm.MongoDB.QueryExecutor
{
    public partial class QueryExecutorArgument
    {
        public virtual void ReverseOrder()
        {
            DbContext dbContext = this.dbContext;
            CombinedStream stream = this.combinedStream;

            stream.orders ??= new();
            var orders = stream.orders;
            // make sure orders exist
            if (!orders.Any())
            {
                AddOrder(stream.source);
                //stream.joins?.ForEach(right => AddOrder(right.right));

                #region AddOrder
                void AddOrder(IStream source)
                {
                    if (source is SourceStream sourceStream)
                    {
                        var entityType = sourceStream.GetEntityType();
                        var entityDescriptor = dbContext.GetEntityDescriptor(entityType);
                        if (entityDescriptor != null)
                        {
                            var parentMember = ExpressionNode.Member(objectValue: null, memberName: null);
                            parentMember.Member_SetType(entityType);

                            var member = ExpressionNode.Member(objectValue: parentMember, memberName: entityDescriptor.key.propertyName);
                            member.Member_SetType(entityDescriptor.key.type);

                            orders.Add(new ExpressionNodeOrderField { member = member, asc = true });
                        }
                    }
                }
                #endregion
            }

            // reverse order
            orders?.ForEach(order => order.asc = !order.asc);
        }
    }

}
