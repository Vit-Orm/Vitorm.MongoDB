using MongoDB.Driver;

using Vitorm.Transaction;

namespace Vitorm.MongoDB.Transaction
{
    public class TransactionManager : ITransactionManager
    {
        public TransactionManager(DbContext dbContext)
        {
            this.dbContext = dbContext;
        }

        protected DbContext dbContext;
        public Transaction transaction;

        public virtual IClientSessionHandle session => transaction?.isActive == true ? transaction.session : null;

        public virtual ITransaction BeginTransaction()
        {
            if (transaction?.isActive == true) throw new System.NotSupportedException("MongoDB do not support nested transaction.");

            transaction = new Transaction(dbContext);
            return transaction;
        }

        public virtual void Dispose()
        {
            if (transaction != null)
            {
                transaction.Dispose();
                transaction = null;
            }
        }


    }

}
