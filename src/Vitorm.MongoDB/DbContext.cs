using System.Data;

namespace Vitorm.MongoDB
{
    public partial class DbContext : Vitorm.DbContext
    {
        public DbConfig dbConfig { get; protected set; }

        public DbContext(DbConfig dbConfig) : base(DbSetConstructor.CreateDbSet)
        {
            this.dbConfig = dbConfig;
        }


        #region Transaction
        public virtual IDbTransaction BeginTransaction() => throw new System.NotImplementedException();
        public virtual IDbTransaction GetCurrentTransaction() => throw new System.NotImplementedException();

        #endregion



        public virtual string databaseName => throw new System.NotImplementedException();
        public virtual void ChangeDatabase(string databaseName) => throw new System.NotImplementedException();

    }
}