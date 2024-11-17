using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

using MongoDB.Driver;

using Vit.Linq.FilterRules;
using Vit.Linq.FilterRules.ComponentModel;

using Vitorm.Entity;

namespace Vitorm.MongoDB
{
    public partial class DbSet_Primitive<Entity> : IDbSet<Entity>
    {
        public virtual IDbContext dbContext { get; protected set; }
        public virtual DbContext DbContext => (DbContext)dbContext;


        protected IEntityDescriptor _entityDescriptor;
        public virtual IEntityDescriptor entityDescriptor => _entityDescriptor;


        public DbSet_Primitive(DbContext dbContext, IEntityDescriptor entityDescriptor)
        {
            this.dbContext = dbContext;
            this._entityDescriptor = entityDescriptor;
        }

        // #0 Schema :  ChangeTable
        public virtual IEntityDescriptor ChangeTable(string tableName) => _entityDescriptor = _entityDescriptor.WithTable(tableName);
        public virtual IEntityDescriptor ChangeTableBack() => _entityDescriptor = _entityDescriptor.GetOriginEntityDescriptor();

        public IMongoDatabase database => DbContext.dbConfig.GetDatabase();
        public IMongoCollection<Entity> collection => database.GetCollection<Entity>(entityDescriptor.tableName);


        #region #0 Schema :  Create Drop

        public virtual bool TableExist()
        {
            var names = database.ListCollectionNames().ToList();
            return names.Exists(name => entityDescriptor.tableName.Equals(name, StringComparison.OrdinalIgnoreCase));
        }
        public virtual async Task<bool> TableExistAsync()
        {
            var names = await (await database.ListCollectionNamesAsync()).ToListAsync();
            return names.Exists(name => entityDescriptor.tableName.Equals(name, StringComparison.OrdinalIgnoreCase));
        }

        public virtual void TryCreateTable()
        {
            if (!TableExist())
                database.CreateCollection(entityDescriptor.tableName);
        }
        public virtual async Task TryCreateTableAsync()
        {
            if (!await TableExistAsync())
                await database.CreateCollectionAsync(entityDescriptor.tableName);
        }

        public virtual void TryDropTable() => database.DropCollection(entityDescriptor.tableName);
        public virtual Task TryDropTableAsync() => database.DropCollectionAsync(entityDescriptor.tableName);


        public virtual void Truncate() => collection.DeleteMany(m => true);

        public virtual Task TruncateAsync() => collection.DeleteManyAsync(m => true);

        #endregion


        #region #1 Create :  Add AddRange
        public virtual Entity Add(Entity entity)
        {
            collection.InsertOne(entity);
            return entity;
        }

        public virtual void AddRange(IEnumerable<Entity> entities)
        {
            collection.InsertMany(entities);
        }

        public virtual async Task<Entity> AddAsync(Entity entity)
        {
            await collection.InsertOneAsync(entity);
            return entity;
        }

        public virtual Task AddRangeAsync(IEnumerable<Entity> entities)
        {
            return collection.InsertManyAsync(entities);
        }
        #endregion


        #region #2 Retrieve : Get Query

        public virtual Entity Get(object keyValue)
        {
            var predicate = GetKeyPredicate(keyValue);
            return collection.Find(predicate).FirstOrDefault();
        }

        public virtual Task<Entity> GetAsync(object keyValue)
        {
            var predicate = GetKeyPredicate(keyValue);
            return collection.Find(predicate).FirstOrDefaultAsync();
        }

        public virtual IQueryable<Entity> Query()
        {
            return collection.AsQueryable();
        }

        #endregion


        public virtual Expression<Func<Entity, bool>> GetKeyPredicate(object keyValue)
        {
            var filter = new FilterRule { field = entityDescriptor.key.propertyName, @operator = "=", value = keyValue };
            var predicate = FilterService.Instance.ConvertToCode_PredicateExpression<Entity>(filter);
            return predicate;
        }

        public virtual Expression<Func<Entity, bool>> GetKeyPredicate<Key>(IEnumerable<Key> keys)
        {
            var filter = new FilterRule { field = entityDescriptor.key.propertyName, @operator = "In", value = keys };
            var predicate = FilterService.Instance.ConvertToCode_PredicateExpression<Entity>(filter);
            return predicate;
        }

        #region #3 Update: Update UpdateRange
        public virtual int Update(Entity entity)
        {
            var predicate = GetKeyPredicate(entityDescriptor.key.GetValue(entity));
            var result = collection.ReplaceOne(predicate, entity);
            return result.IsAcknowledged && result.IsModifiedCountAvailable ? (int)result.ModifiedCount : 0;
        }
        public virtual async Task<int> UpdateAsync(Entity entity)
        {
            var predicate = GetKeyPredicate(entityDescriptor.key.GetValue(entity));
            var result = await collection.ReplaceOneAsync(predicate, entity);
            return result.IsAcknowledged && result.IsModifiedCountAvailable ? (int)result.ModifiedCount : 0;
        }

        public virtual int UpdateRange(IEnumerable<Entity> entities)
        {
            return entities.Select(entity => Update(entity)).Sum();
        }
        public virtual async Task<int> UpdateRangeAsync(IEnumerable<Entity> entities)
        {
            int count = 0;
            foreach (var entity in entities)
                count += await UpdateAsync(entity);
            return count;
        }
        #endregion


        #region #4 Delete : Delete DeleteRange DeleteByKey DeleteByKeys

        public virtual int Delete(Entity entity)
        {
            var keyValue = entityDescriptor.key.GetValue(entity);
            return DeleteByKey(keyValue);
        }
        public virtual Task<int> DeleteAsync(Entity entity)
        {
            var keyValue = entityDescriptor.key.GetValue(entity);
            return DeleteByKeyAsync(keyValue);
        }



        public virtual int DeleteRange(IEnumerable<Entity> entities)
        {
            var keys = entities.Select(entity => entityDescriptor.key.GetValue(entity));
            return DeleteByKeys(keys);
        }
        public virtual Task<int> DeleteRangeAsync(IEnumerable<Entity> entities)
        {
            var keys = entities.Select(entity => entityDescriptor.key.GetValue(entity));
            return DeleteByKeysAsync<object>(keys);
        }



        public virtual int DeleteByKey(object keyValue)
        {
            var predicate = GetKeyPredicate(keyValue);
            var result = collection.DeleteOne(predicate);
            return result.IsAcknowledged ? (int)result.DeletedCount : 0;
        }
        public virtual async Task<int> DeleteByKeyAsync(object keyValue)
        {
            var predicate = GetKeyPredicate(keyValue);
            var result = await collection.DeleteOneAsync(predicate);
            return result.IsAcknowledged ? (int)result.DeletedCount : 0;
        }



        public virtual int DeleteByKeys<Key>(IEnumerable<Key> keys)
        {
            var predicate = GetKeyPredicate<Key>(keys);
            var result = collection.DeleteMany(predicate);
            return result.IsAcknowledged ? (int)result.DeletedCount : 0;
        }
        public virtual async Task<int> DeleteByKeysAsync<Key>(IEnumerable<Key> keys)
        {
            var predicate = GetKeyPredicate<Key>(keys);
            var result = await collection.DeleteManyAsync(predicate);
            return result.IsAcknowledged ? (int)result.DeletedCount : 0;
        }

        #endregion




    }
}
