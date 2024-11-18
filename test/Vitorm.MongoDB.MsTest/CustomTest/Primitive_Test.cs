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





    }
}
