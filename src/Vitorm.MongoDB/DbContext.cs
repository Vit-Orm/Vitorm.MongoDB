namespace Vitorm.MongoDB
{
    public partial class DbContext : Vitorm.DbContext
    {
        public DbConfig dbConfig { get; protected set; }

        public DbContext(DbConfig dbConfig) : base(DbSetConstructor.CreateDbSet)
        {
            this.dbConfig = dbConfig;
        }

    }
}