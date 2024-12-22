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
            var collectionNames = DbContext.session == null ? database.ListCollectionNames().ToList() : database.ListCollectionNames(DbContext.session).ToList();
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
            var collectionNames = DbContext.session == null ? await (await database.ListCollectionNamesAsync()).ToListAsync() : await (await database.ListCollectionNamesAsync(DbContext.session)).ToListAsync();
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

            if (DbContext.session == null)
                collection.Indexes.CreateOne(indexModel);
            else
                collection.Indexes.CreateOne(DbContext.session, indexModel);
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

            if (DbContext.session == null)
                await collection.Indexes.CreateOneAsync(indexModel);
            else
                await collection.Indexes.CreateOneAsync(DbContext.session, indexModel);

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


            if (DbContext.session == null)
                database.CreateCollection(entityDescriptor.tableName);
            else
                database.CreateCollection(DbContext.session, entityDescriptor.tableName);


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


            if (DbContext.session == null)
                await database.CreateCollectionAsync(entityDescriptor.tableName);
            else
                await database.CreateCollectionAsync(DbContext.session, entityDescriptor.tableName);


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

            if (DbContext.session == null)
                database.DropCollection(entityDescriptor.tableName);
            else
                database.DropCollection(DbContext.session, entityDescriptor.tableName);

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


            if (DbContext.session == null)
                await database.DropCollectionAsync(entityDescriptor.tableName);
            else
                await database.DropCollectionAsync(DbContext.session, entityDescriptor.tableName);
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

            if (DbContext.session == null)
                collection.DeleteMany(m => true);
            else
                collection.DeleteMany(DbContext.session, m => true);

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


            if (DbContext.session == null)
                await collection.DeleteManyAsync(m => true);
            else
                await collection.DeleteManyAsync(DbContext.session, m => true);

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

            if (DbContext.session == null)
                collection.InsertOne(doc);
            else
                collection.InsertOne(DbContext.session, doc);

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

            if (DbContext.session == null)
                await collection.InsertOneAsync(doc);
            else
                await collection.InsertOneAsync(DbContext.session, doc);

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

            if (DbContext.session == null)
                collection.InsertMany(docs);
            else
                collection.InsertMany(DbContext.session, docs);

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

            if (DbContext.session == null)
                await collection.InsertManyAsync(docs);
            else
                await collection.InsertManyAsync(DbContext.session, docs);

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

            var fluent = DbContext.session == null ? collection.Find(predicate) : collection.Find(DbContext.session, predicate);

            return Deserialize(fluent.FirstOrDefault());
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
            var fluent = DbContext.session == null ? await collection.FindAsync(predicate) : await collection.FindAsync(DbContext.session, predicate);
            return Deserialize(await fluent.FirstOrDefaultAsync());
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

            var result = DbContext.session == null ? collection.ReplaceOne(predicate, doc) : collection.ReplaceOne(DbContext.session, predicate, doc);
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

            var result = DbContext.session == null ? await collection.ReplaceOneAsync(predicate, doc) : await collection.ReplaceOneAsync(DbContext.session, predicate, doc);
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

            var result = DbContext.session == null ? collection.DeleteOne(predicate) : collection.DeleteOne(DbContext.session, predicate);
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

            var result = DbContext.session == null ? await collection.DeleteOneAsync(predicate) : await collection.DeleteOneAsync(DbContext.session, predicate);
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

            var result = DbContext.session == null ? collection.DeleteMany(predicate) : collection.DeleteMany(DbContext.session, predicate);
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

            var result = DbContext.session == null ? await collection.DeleteManyAsync(predicate) : await collection.DeleteManyAsync(DbContext.session, predicate);
            return result.IsAcknowledged ? (int)result.DeletedCount : 0;
        }

        #endregion




    }
}
