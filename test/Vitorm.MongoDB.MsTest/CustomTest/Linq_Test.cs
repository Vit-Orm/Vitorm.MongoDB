using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using MongoDB.Driver;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Linq_Test
    {
        [System.ComponentModel.DataAnnotations.Schema.Table("User2")]
        public class User2 : User
        {

            public string[] nickNames { get; set; }
            public List<string> nickNameList { get; set; }
            public SortedSet<string> nickNameList2 { get; set; }

            public float?[] scores { get; set; }
            public int[] scoreRanks { get; set; }

            public List<float?> scoreList { get; set; }
            public List<int> scoreRankList { get; set; }

            public User2[] parents { get; set; }
            public List<User2> parentList { get; set; }

        }


        static Vitorm.MongoDB.DbContext CreateDbContext()
        {
            using var dbContext = DataSource.CreateDbContext(autoInit: false);

            //return dbContext;

            dbContext.TryDropTable<User2>();
            dbContext.TryCreateTable<User2>();

            var users = new List<User2> {
                    new User2 { id=1, name="u146", fatherId=4, motherId=6 },
                    new User2 { id=2, name="u246", fatherId=4, motherId=6 },
                    new User2 { id=3, name="u356", fatherId=5, motherId=6 },
                    new User2 { id=4, name="u400" },
                    new User2 { id=5, name="u500" },
                    new User2 { id=6, name="u600" },
                };

            users.ForEach(user =>
            {
                user.birth = DateTime.Parse("2021-01-01 00:00:00").AddHours(user.id);
                user.classId = user.id % 2 + 1;


                if (user.fatherId.HasValue || user.motherId.HasValue)
                {
                    user.parents = new User2[] {
                            user.fatherId.HasValue? new (){ id=user.fatherId.Value}:null,
                             user.motherId.HasValue? new (){ id=user.motherId.Value}:null
                    };
                    user.parentList = new(user.parents);

                    user.father = user.parents?.FirstOrDefault();
                }

                user.nickNames = new[] { user.name, user.fatherId?.ToString(), user.motherId?.ToString() };
                user.nickNameList = new(user.nickNames);
                user.nickNameList2 = new(user.nickNames);

                user.scores = new float?[] { user.id * 10.1f, null, user.id * 10.2f };
                user.scoreList = new(user.scores);

                user.scoreRanks = new[] { user.id, user.id + 100 };
                user.scoreRankList = new(user.scoreRanks);

            });

            dbContext.AddRange(users);

            return dbContext;
        }



        [TestMethod]
        public async Task NestedEntity()
        {
            using var dbContext = CreateDbContext();

            {
                var list = dbContext.Query<User2>().ToList();
                Assert.AreEqual(6, list.Count);
            }

            {
                var list = dbContext.Query<User2>().Where(m => m.id != 1).ToList();
                Assert.AreEqual(5, list.Count);
            }
        }


        [TestMethod]
        public async Task Test_Select()
        {
            using var dbContext = CreateDbContext();

            {
                var list = dbContext.Query<User2>().Where(m => m.id != 1).Select(m => m).ToList();
                Assert.AreEqual(5, list.Count);
            }

            {
                var list = dbContext.Query<User2>().Where(m => m.id != 1).Select(m => m.id).ToList();
                Assert.AreEqual(5, list.Count);
            }

            {
                var list = dbContext.Query<User2>().Where(m => m.id != 1).Select(m => m.fatherId + 1).ToList();
                Assert.AreEqual(5, list.Count);
            }

            {
                var list = dbContext.Query<User2>().Where(m => m.id != 1).Select(m => new { m.id, m.father, m.name, id2 = m.id + 1 }).ToList();
                Assert.AreEqual(5, list.Count);
            }

        }

        [TestMethod]
        public async Task Test_OrderSkipTake()
        {
            using var dbContext = CreateDbContext();

            {
                var list = dbContext.Query<User2>().OrderBy(m => m.id).ThenByDescending(m => m.fatherId).Select(m => new { m.id, m.fatherId }).ToList();
                var ids = String.Join(',', list.Select(m => m.id));
                Assert.AreEqual(6, list.Count);
                Assert.AreEqual("1,2,3,4,5,6", ids);
            }

            {
                var list = dbContext.Query<User2>().OrderBy(m => m.fatherId).ThenByDescending(m => m.id).Select(m => new { m.id, m.fatherId }).ToList();
                var ids = String.Join(',', list.Select(m => m.id));
                Assert.AreEqual(6, list.Count);
                Assert.AreEqual("6,5,4,2,1,3", ids);
            }
            {
                var list = dbContext.Query<User2>().OrderBy(m => m.father.id).ThenBy(m => m.id).Select(m => new { m.id, m.fatherId }).ToList();
                var ids = String.Join(',', list.Select(m => m.id));
                Assert.AreEqual(6, list.Count);
                Assert.AreEqual("4,5,6,1,2,3", ids);
            }



            {
                var list = dbContext.Query<User2>().OrderBy(m => m.father.id).ThenBy(m => m.id).Skip(1).Take(3).Select(m => new { m.id, m.fatherId }).ToList();
                var ids = String.Join(',', list.Select(m => m.id));
                Assert.AreEqual(3, list.Count);
                Assert.AreEqual("5,6,1", ids);
            }

            {
                var list = dbContext.Query<User2>().OrderBy(m => m.father.id).ThenBy(m => m.id).Skip(0).Take(10).Select(m => new { m.id, m.fatherId }).ToList();
                var ids = String.Join(',', list.Select(m => m.id));
                Assert.AreEqual(6, list.Count);
                Assert.AreEqual("4,5,6,1,2,3", ids);
            }

        }



        [TestMethod]
        public async Task Test_Where()
        {
            using var dbContext = CreateDbContext();

            {
                var list = dbContext.Query<User2>().Where(m => m.fatherId == 4).OrderBy(m => m.id).ToList();
                var ids = String.Join(',', list.Select(m => m.id));
                Assert.AreEqual(2, list.Count);
                Assert.AreEqual("1,2", ids);
            }

            {
                var list = dbContext.Query<User2>().Where(m => m.father.id == 4).OrderBy(m => m.id).ToList();
                var ids = String.Join(',', list.Select(m => m.id));
                Assert.AreEqual(2, list.Count);
                Assert.AreEqual("1,2", ids);
            }

            {
                var list = dbContext.Query<User2>().Where(m => m.father.id != 4).OrderBy(m => m.id).ToList();
                var ids = String.Join(',', list.Select(m => m.id));
                Assert.AreEqual(4, list.Count);
                Assert.AreEqual("3,4,5,6", ids);
            }


            {
                var list = dbContext.Query<User2>().Where(m => m.parents[0].id == 4).OrderBy(m => m.id).ToList();
                var ids = String.Join(',', list.Select(m => m.id));
                Assert.AreEqual(2, list.Count);
                Assert.AreEqual("1,2", ids);
            }

            {
                var list = dbContext.Query<User2>().Where(m => m.parentList[0].id == 4).OrderBy(m => m.id).ToList();
                var ids = String.Join(',', list.Select(m => m.id));
                Assert.AreEqual(2, list.Count);
                Assert.AreEqual("1,2", ids);
            }

        }


    }
}
