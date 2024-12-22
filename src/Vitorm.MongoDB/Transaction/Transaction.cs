using MongoDB.Driver;

using Vitorm.Transaction;

namespace Vitorm.MongoDB.Transaction
{
    public partial class Transaction : ITransaction
    {
        public IClientSessionHandle session { get; protected set; }
        public bool isActive { get; protected set; }
        public Transaction(DbContext dbContext)
        {
            session = dbContext.dbConfig.Client.StartSession();
            session.StartTransaction();
            isActive = true;
        }


        public virtual void Commit()
        {
            if (!isActive) throw new System.InvalidOperationException("MongoDB Transaction is not started.");

            session.CommitTransaction();
            session.Dispose();
            session = null;

            isActive = false;

        }
        public virtual void Dispose()
        {
            if (session == null) return;

            if (isActive)
            {
                session.AbortTransaction();
                isActive = false;
            }

            session.Dispose();
            session = null;
        }

        public virtual void Rollback()
        {
            if (!isActive) throw new System.InvalidOperationException("MongoDB Transaction is not started.");

            session.AbortTransaction();
            session.Dispose();
            session = null;

            isActive = false;
        }
    }

}
