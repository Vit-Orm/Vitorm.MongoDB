using Microsoft.VisualStudio.TestTools.UnitTesting;

using MongoDB.Bson;
using MongoDB.Driver;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Primitive_Test
    {

        [TestMethod]
        public async Task Test()
        {
            using var dbContext = DataSource.CreateDbContext(autoInit: false);

            var database = dbContext.dbConfig.GetDatabase();

            // #1 Create table
            if (database.ListCollectionNames().ToList().Contains("User2", StringComparer.OrdinalIgnoreCase))
                await database.DropCollectionAsync("User2");

            await database.CreateCollectionAsync("User2");


            var collection = database.GetCollection<BsonDocument>("User2");


            // #2 Add
            {
                var user = new BsonDocument
                {
                    ["userId"] = BsonValue.Create(1),
                    ["userName"] = BsonValue.Create("u146"),
                    ["fatherId"] = BsonValue.Create(4),

                    ["father"] = new BsonDocument
                    {
                        ["userId"] = BsonValue.Create(4),
                        ["userName"] = BsonValue.Create("u400"),
                    },
                    ["parents"] = new BsonArray
                        {
                           new BsonDocument
                            {
                                ["userId"] = BsonValue.Create(4),
                                ["userName"] = BsonValue.Create("u400"),
                            }
                        },
                };

                await collection.InsertOneAsync(user);
            }


            // #3 query by FilterBuilder
            {
                var builder = Builders<BsonDocument>.Filter;

                {
                    var filter = builder.Eq("userId", 1);
                    var json = collection.Find(filter).FirstOrDefault()?.ToJson();
                    Assert.IsNotNull(json);
                }

                {
                    var filter = builder.And(builder.Eq("userId", 1), builder.Regex("userName", ",*146"));
                    var json = collection.Find(filter).FirstOrDefault()?.ToJson();
                    Assert.IsNotNull(json);
                }

                {
                    var filter = builder.Eq("parents.0.userId", 4);
                    var json = collection.Find(filter).FirstOrDefault()?.ToJson();
                    Assert.IsNotNull(json);
                }
            }

        }



        [TestMethod]
        public async Task GroupTest()
        {
            using var dbContext = DataSource.CreateDbContext(autoInit: true);

            var database = dbContext.dbConfig.GetDatabase();
            var collection = database.GetCollection<BsonDocument>("User");

            // group
            {
                /* -- MySql
                    select userFatherId,userMotherId ,count(*) as count from `User` 
                    where userId >1
                    group by userFatherId ,userMotherId 
                    having count(*)>=1
                    order by count(*);
                */
                // Aggregation pipeline
                var pipeline = new[]
                {
                    new BsonDocument("$match", new BsonDocument("userId", new BsonDocument("$gt", 1))),
                    new BsonDocument("$group", new BsonDocument
                    {
                        { "_id", new BsonDocument { { "userFatherId", "$userFatherId" }, { "userMotherId", "$userMotherId" } } },
                        { "count", new BsonDocument("$sum", 1) }
                    }),
                    new BsonDocument("$match", new BsonDocument("count", new BsonDocument("$gte", 1))),
                    new BsonDocument("$sort", new BsonDocument("count", -1)),
                    new BsonDocument("$project", new BsonDocument
                    {
                        { "userFatherId", "$_id.userFatherId" },
                        { "userMotherId", "$_id.userMotherId" },
                        { "count", 1 },
                        { "_id", 0 }
                    })
                };


                // Execute the aggregation
                using var results = await collection.AggregateAsync<BsonDocument>(pipeline);
                var json = results.ToList()?.ToJson();
            }

        }





        [TestMethod]
        public async Task TransactionTest()
        {
            using var dbContext = DataSource.CreateDbContext(autoInit: false);

            var database = dbContext.dbConfig.GetDatabase();

            // #1 Create table
            if (database.ListCollectionNames().ToList().Contains("User2", StringComparer.OrdinalIgnoreCase))
                await database.DropCollectionAsync("User2");

            await database.CreateCollectionAsync("User2");
            var collection = database.GetCollection<BsonDocument>("User2");

            // transaction
            {
                {
                    using var session = dbContext.dbConfig.Client.StartSession();
                    session.StartTransaction();
                    try
                    {
                        var user = new BsonDocument
                        {
                            ["userId"] = BsonValue.Create(1),
                        };

                        await collection.InsertOneAsync(session, user);

                        // Commits our transaction
                        await session.CommitTransactionAsync();
                    }
                    catch (Exception e)
                    {
                        await session.AbortTransactionAsync();
                    }
                }

                // assert
                var list = collection.AsQueryable().ToList();
                Assert.AreEqual(1, list.Count);
            }

            // transaction
            {
                {
                    using var session = dbContext.dbConfig.Client.StartSession();
                    session.StartTransaction();

                    var user = new BsonDocument
                    {
                        ["userId"] = BsonValue.Create(2),
                    };

                    await collection.InsertOneAsync(session, user);
                    await session.AbortTransactionAsync();
                }

                // assert
                var list = collection.AsQueryable().ToList();
                Assert.AreEqual(1, list.Count);
            }



            // transaction
            {
                {
                    using var session = dbContext.dbConfig.Client.StartSession();
                    session.StartTransaction();

                    var user = new BsonDocument
                    {
                        ["userId"] = BsonValue.Create(2),
                    };

                    await collection.InsertOneAsync(session, user);
                    //await session.AbortTransactionAsync();
                }

                // assert
                var list = collection.AsQueryable().ToList();
                Assert.AreEqual(1, list.Count);
            }



        }






    }
}
