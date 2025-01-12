using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vit.Linq;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Query_Method_Test
    {

        [TestMethod]
        public async Task ToListAsync()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            // PlainQuery
            {
                var list = await userQuery.OrderBy(m => m.fatherId).Select(m => new { fatherId = m.fatherId }).ToListAsync();
                var ids = String.Join(',', list.Select(m => m.fatherId));
                Assert.AreEqual(",,,4,4,5", ids);
            }

            // Group
            {
                var list = await userQuery.GroupBy(m => m.fatherId).OrderBy(g => g.Key).Select(g => new { fatherId = g.Key }).ToListAsync();
                var ids = String.Join(',', list.Select(m => m.fatherId));
                Assert.AreEqual(",4,5", ids);
            }

            // PlainDistinctSearch
            {
                var list = await userQuery.Select(m => new { fatherId = m.fatherId }).OrderBy(m => m.fatherId).Distinct().ToListAsync();
                var ids = String.Join(',', list.Select(m => m.fatherId));
                Assert.AreEqual(",4,5", ids);
            }
        }



        [TestMethod]
        public void ToList()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            // PlainQuery
            {
                var list = userQuery.OrderBy(m => m.fatherId).Select(m => new { fatherId = m.fatherId }).ToList();
                var ids = String.Join(',', list.Select(m => m.fatherId));
                Assert.AreEqual(",,,4,4,5", ids);
            }

            // Group
            {
                var list = userQuery.GroupBy(m => m.fatherId).OrderBy(g => g.Key).Select(g => new { fatherId = g.Key }).ToList();
                var ids = String.Join(',', list.Select(m => m.fatherId));
                Assert.AreEqual(",4,5", ids);
            }

            // PlainDistinctSearch
            {
                var list = userQuery.Select(m => new { fatherId = m.fatherId }).OrderBy(m => m.fatherId).Distinct().ToList();
                var ids = String.Join(',', list.Select(m => m.fatherId));
                Assert.AreEqual(",4,5", ids);
            }
        }


        [TestMethod]
        public void Count()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            // PlainQuery
            {
                var count = userQuery.OrderBy(m => m.fatherId).Select(m => new { fatherId = m.fatherId }).Count();
                Assert.AreEqual(6, count);
            }

            // Group
            {
                var count = userQuery.GroupBy(m => m.fatherId).OrderBy(g => g.Key).Select(g => new { fatherId = g.Key }).Count();
                Assert.AreEqual(3, count);
            }

            // PlainDistinctSearch
            {
                var count = userQuery.Select(m => new { fatherId = m.fatherId }).OrderBy(m => m.fatherId).Distinct().Count();
                Assert.AreEqual(3, count);
            }
        }


        [TestMethod]
        public void ToExecuteString()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            // PlainQuery
            {
                var executeString = userQuery.OrderBy(m => m.fatherId).Select(m => new { fatherId = m.fatherId }).ToExecuteString();
                Assert.IsNotNull(executeString);
            }

            // Group
            {
                var executeString = userQuery.GroupBy(m => m.fatherId).OrderBy(g => g.Key).Select(g => new { fatherId = g.Key }).ToExecuteString();
                Assert.IsNotNull(executeString);
            }

            // PlainDistinctSearch
            {
                var executeString = userQuery.Select(m => new { fatherId = m.fatherId }).OrderBy(m => m.fatherId).Distinct().ToExecuteString();
                Assert.IsNotNull(executeString);
            }
        }



        [TestMethod]
        public void FirstOrDefault()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            // PlainQuery
            {
                var item = userQuery.Where(m => m.fatherId != null).OrderBy(m => m.fatherId).Select(m => new { fatherId = m.fatherId }).FirstOrDefault();
                Assert.AreEqual(4, item.fatherId);
            }

            // Group
            {
                var item = userQuery.Where(m => m.fatherId != null).GroupBy(m => m.fatherId).OrderBy(g => g.Key).Select(g => new { fatherId = g.Key }).FirstOrDefault();
                Assert.AreEqual(4, item.fatherId);
            }

            // PlainDistinctSearch
            {
                var item = userQuery.Where(m => m.fatherId != null).Select(m => new { fatherId = m.fatherId }).OrderBy(m => m.fatherId).Distinct().FirstOrDefault();
                Assert.AreEqual(4, item.fatherId);
            }
        }


        [TestMethod]
        public void LastOrDefault()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            // PlainQuery
            {
                var item = userQuery.Where(m => m.fatherId != null).OrderBy(m => m.fatherId).Select(m => new { fatherId = m.fatherId }).LastOrDefault();
                Assert.AreEqual(5, item.fatherId);
            }

            // Group
            {
                var item = userQuery.Where(m => m.fatherId != null).GroupBy(m => m.fatherId).OrderBy(g => g.Key).Select(g => new { fatherId = g.Key }).LastOrDefault();
                Assert.AreEqual(5, item.fatherId);
            }

            // PlainDistinctSearch
            {
                var item = userQuery.Where(m => m.fatherId != null).Select(m => new { fatherId = m.fatherId }).OrderBy(m => m.fatherId).Distinct().LastOrDefault();
                Assert.AreEqual(5, item.fatherId);
            }
        }


    }
}
