using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

using Vitorm.Entity;

namespace Vitorm.MongoDB
{
    public partial class DbSet<Entity> : IDbSet<Entity>
    {
        public virtual IDbContext dbContext { get; protected set; }
        public virtual DbContext DbContext => (DbContext)dbContext;


        protected IEntityDescriptor _entityDescriptor;
        public virtual IEntityDescriptor entityDescriptor => _entityDescriptor;


        public DbSet(DbContext dbContext, IEntityDescriptor entityDescriptor)
        {
            this.dbContext = dbContext;
            this._entityDescriptor = entityDescriptor;
        }

        // #0 Schema :  ChangeTable
        public virtual IEntityDescriptor ChangeTable(string tableName) => _entityDescriptor = _entityDescriptor.WithTable(tableName);
        public virtual IEntityDescriptor ChangeTableBack() => _entityDescriptor = _entityDescriptor.GetOriginEntityDescriptor();

        public IMongoDatabase database => DbContext.dbConfig.GetDatabase();

        public IMongoCollection<BsonDocument> collection => database.GetCollection<BsonDocument>(entityDescriptor.tableName);



        public virtual BsonDocument Serialize(Entity entity)
        {
            return DbContext.Serialize(entity, entityDescriptor);
        }
        public virtual Entity Deserialize(BsonDocument doc)
        {
            return (Entity)DbContext.Deserialize(doc, entityDescriptor);
        }




        #region #0 Schema :  Create Drop

        public virtual bool TableExists()
        {
            var collectionNames = database.ListCollectionNames().ToList();
            var exists = collectionNames.Exists(name => entityDescriptor.tableName.Equals(name, StringComparison.OrdinalIgnoreCase));

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: entityDescriptor.tableName,
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "TableExists",
                    ["collectionNames"] = collectionNames,
                    ["exists"] = exists
                }))
             );

            return exists;
        }
        public virtual async Task<bool> TableExistsAsync()
        {
            var collectionNames = await (await database.ListCollectionNamesAsync()).ToListAsync();
            var exists = collectionNames.Exists(name => entityDescriptor.tableName.Equals(name, StringComparison.OrdinalIgnoreCase));

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: entityDescriptor.tableName,
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "TableExistsAsync",
                    ["collectionNames"] = collectionNames,
                    ["exists"] = exists
                }))
            );

            return exists;
        }



        public virtual void CreateIndex(string field, bool ascending = true, bool unique = false)
        {
            // https://www.mongodb.com/docs/drivers/csharp/current/fundamentals/indexes/
            var options = new CreateIndexOptions { Unique = true };
            var indexModel = new CreateIndexModel<BsonDocument>(Builders<BsonDocument>.IndexKeys.Ascending(entityDescriptor.key.columnName), options);

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: field,
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "CreateIndex",
                    ["indexModel"] = indexModel
                }))
            );

            collection.Indexes.CreateOne(indexModel);
        }
        public virtual async Task CreateIndexAsync(string field, bool ascending = true, bool unique = false)
        {
            var options = new CreateIndexOptions { Unique = true };
            var indexModel = new CreateIndexModel<BsonDocument>(Builders<BsonDocument>.IndexKeys.Ascending(entityDescriptor.key.columnName), options);

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: field,
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "CreateIndexAsync",
                    ["indexModel"] = indexModel
                }))
            );

            await collection.Indexes.CreateOneAsync(indexModel);
        }



        public virtual void TryCreateTable()
        {
            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: entityDescriptor.tableName,
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "TryCreateTable"
                }))
            );


            if (TableExists()) return;

            database.CreateCollection(entityDescriptor.tableName);

            // create unique index
            if (entityDescriptor.key != null && entityDescriptor.key.columnName != "_id")
            {
                CreateIndex(entityDescriptor.key.columnName, ascending: true, unique: true);
            }
        }


        public virtual async Task TryCreateTableAsync()
        {
            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: entityDescriptor.tableName,
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "TryCreateTableAsync"
                }))
            );

            if (await TableExistsAsync()) return;

            await database.CreateCollectionAsync(entityDescriptor.tableName);

            // create unique index
            if (entityDescriptor.key != null && entityDescriptor.key.columnName != "_id")
            {
                await CreateIndexAsync(entityDescriptor.key.columnName, ascending: true, unique: true);
            }
        }

        public virtual void TryDropTable()
        {
            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: entityDescriptor.tableName,
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "TryDropTable"
                }))
            );

            database.DropCollection(entityDescriptor.tableName);
        }
        public virtual async Task TryDropTableAsync()
        {
            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: entityDescriptor.tableName,
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "TryDropTableAsync"
                }))
            );

            await database.DropCollectionAsync(entityDescriptor.tableName);
        }

        public virtual void Truncate()
        {
            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: entityDescriptor.tableName,
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "Truncate"
                }))
            );

            collection.DeleteMany(m => true);
        }
        public virtual async Task TruncateAsync()
        {
            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: entityDescriptor.tableName,
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "TruncateAsync"
                }))
            );

            await collection.DeleteManyAsync(m => true);
        }
        #endregion


        #region #1 Create :  Add AddRange
        public virtual Entity Add(Entity entity)
        {
            var doc = DbContext.Serialize(entity, entityDescriptor);

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: doc?.ToJson(),
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "Add",
                    ["entity"] = entity,
                    ["doc"] = doc
                }))
            );

            collection.InsertOne(doc);
            return entity;
        }


        public virtual async Task<Entity> AddAsync(Entity entity)
        {
            var doc = DbContext.Serialize(entity, entityDescriptor);

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: doc?.ToJson(),
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "AddAsync",
                    ["entity"] = entity,
                    ["doc"] = doc
                }))
            );

            await collection.InsertOneAsync(doc);
            return entity;
        }

        public virtual void AddRange(IEnumerable<Entity> entities)
        {
            var docs = entities.Select(entity => DbContext.Serialize(entity, entityDescriptor));

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: new BsonArray(docs)?.ToJson(),
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "AddRange",
                    ["entities"] = entities,
                    ["docs"] = docs
                }))
            );

            collection.InsertMany(docs);
        }


        public virtual async Task AddRangeAsync(IEnumerable<Entity> entities)
        {
            var docs = entities.Select(entity => DbContext.Serialize(entity, entityDescriptor));

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: new BsonArray(docs)?.ToJson(),
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "AddRangeAsync",
                    ["entities"] = entities,
                    ["docs"] = docs
                }))
            );

            await collection.InsertManyAsync(docs);
        }
        #endregion


        #region #2 Retrieve : Get Query

        public virtual Entity Get(object keyValue)
        {
            var predicate = GetKeyPredicate(keyValue);

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: predicate?.ToJson(),
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "Get"
                }))
            );

            return Deserialize(collection.Find(predicate).FirstOrDefault());
        }

        public virtual async Task<Entity> GetAsync(object keyValue)
        {
            var predicate = GetKeyPredicate(keyValue);

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: predicate?.ToJson(),
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "GetAsync"
                }))
            );

            return Deserialize(await collection.Find(predicate).FirstOrDefaultAsync());
        }

        #endregion


        public virtual BsonDocument GetKeyPredicate(object keyValue)
        {
            return new BsonDocument { [entityDescriptor.key.columnName] = BsonValue.Create(keyValue) };
        }

        public virtual BsonDocument GetKeyPredicate<Key>(IEnumerable<Key> keys)
        {
            var values = keys.Select(key => BsonValue.Create(key));
            return new BsonDocument { [entityDescriptor.key.columnName] = new BsonDocument("$in", new BsonArray(values)) };
        }

        #region #3 Update: Update UpdateRange
        public virtual int Update(Entity entity)
        {
            var predicate = GetKeyPredicate(entityDescriptor.key.GetValue(entity));
            var doc = Serialize(entity);

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: doc?.ToJson(),
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "Update",
                    ["predicate"] = predicate,
                    ["entity"] = entity,
                    ["doc"] = doc,
                })));


            var result = collection.ReplaceOne(predicate, doc);
            return result.IsAcknowledged && result.IsModifiedCountAvailable ? (int)result.ModifiedCount : 0;
        }
        public virtual async Task<int> UpdateAsync(Entity entity)
        {
            var predicate = GetKeyPredicate(entityDescriptor.key.GetValue(entity));
            var doc = Serialize(entity);

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: doc?.ToJson(),
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "UpdateAsync",
                    ["predicate"] = predicate,
                    ["entity"] = entity,
                    ["doc"] = doc,
                })));

            var result = await collection.ReplaceOneAsync(predicate, doc);
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

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: predicate?.ToJson(),
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "DeleteByKey",
                    ["predicate"] = predicate,
                    ["key"] = keyValue,
                })));

            var result = collection.DeleteOne(predicate);
            return result.IsAcknowledged ? (int)result.DeletedCount : 0;
        }
        public virtual async Task<int> DeleteByKeyAsync(object keyValue)
        {
            var predicate = GetKeyPredicate(keyValue);

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: predicate?.ToJson(),
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "DeleteByKeyAsync",
                    ["predicate"] = predicate,
                    ["key"] = keyValue,
                })));

            var result = await collection.DeleteOneAsync(predicate);
            return result.IsAcknowledged ? (int)result.DeletedCount : 0;
        }



        public virtual int DeleteByKeys<Key>(IEnumerable<Key> keys)
        {
            var predicate = GetKeyPredicate<Key>(keys);

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: predicate?.ToJson(),
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "DeleteByKeys",
                    ["predicate"] = predicate,
                    ["keys"] = keys,
                })));

            var result = collection.DeleteMany(predicate);
            return result.IsAcknowledged ? (int)result.DeletedCount : 0;
        }
        public virtual async Task<int> DeleteByKeysAsync<Key>(IEnumerable<Key> keys)
        {
            var predicate = GetKeyPredicate<Key>(keys);

            // Event_OnExecuting
            DbContext.Event_OnExecuting(new Lazy<ExecuteEventArgument>(() => new ExecuteEventArgument(
                dbContext: DbContext,
                executeString: predicate?.ToJson(),
                extraParam: new()
                {
                    ["dbSet"] = this,
                    ["entityDescriptor"] = entityDescriptor,
                    ["Method"] = "DeleteByKeysAsync",
                    ["predicate"] = predicate,
                    ["keys"] = keys,
                })));

            var result = await collection.DeleteManyAsync(predicate);
            return result.IsAcknowledged ? (int)result.DeletedCount : 0;
        }

        #endregion




    }
}
