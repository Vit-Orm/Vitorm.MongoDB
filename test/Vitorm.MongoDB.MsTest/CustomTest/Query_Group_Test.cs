using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;


namespace Vitorm.MsTest.CustomTest
{
    [TestClass]
    public class Query_Group_Test
    {

        [TestMethod]
        public void Test_Group_Demo()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            // Linq Expression
            {
                var query =
                        from user in userQuery
                        where user.id > 1 && user.id != 0
                        group user by new { fatherId2 = user.father.id, user.motherId } into userGroup
                        where userGroup.Key.fatherId2 != 4
                        orderby userGroup.Key.fatherId2 descending
                        select new { userGroup.Key.fatherId2, userGroup.Key.motherId };

                var rows = query.ToList();

                Assert.AreEqual(3, rows.Count);
                //Assert.AreEqual(0, rows.Select(u => u.fatherId).Except(new int?[] { 4, 5, null }).Count());
                Assert.AreEqual(0, rows.Select(u => u.motherId).Except(new int?[] { 6, null }).Count());
            }

        }






    }
}
