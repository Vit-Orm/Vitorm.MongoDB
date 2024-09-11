using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

using Vit.Core.Util.ConfigurationManager;

using Vitorm.MongoDB;

namespace Vitorm.MsTest
{
    [System.ComponentModel.DataAnnotations.Schema.Table("User")]
    public class User
    {
        public User() { }

        public User(int id) { this.id = id; }
        public User(string name) { this.name = name; }


        [System.ComponentModel.DataAnnotations.Key]
        [System.ComponentModel.DataAnnotations.Schema.Column("userId")]
        [BsonId]
        [BsonElement("userId")]
        //[BsonRepresentation(BsonType.Int32)]
        public int id { get; set; }

        [System.ComponentModel.DataAnnotations.Schema.Column("userName")]
        [BsonElement("userName")]
        public string name { get; set; }

        //[BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime? birth { get; set; }

        public int? fatherId { get; set; }


        public int? motherId { get; set; }

        // [BsonIgnoreIfNull]
        public int? classId { get; set; }

        [System.ComponentModel.DataAnnotations.Schema.NotMapped]
        [BsonIgnore]
        public string test { get; set; }


        public static User NewUser(int id, bool forAdd = false) => new User { id = id, name = "testUser" + id };

        public static List<User> NewUsers(int startId, int count = 1, bool forAdd = false)
        {
            return Enumerable.Range(startId, count).Select(id => NewUser(id, forAdd)).ToList();
        }
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("UserClass")]
    public class UserClass
    {
        [System.ComponentModel.DataAnnotations.Key]
        [System.ComponentModel.DataAnnotations.Schema.Column("classId")]
        [BsonId]
        [BsonElement("classId")]
        public int id { get; set; }

        [System.ComponentModel.DataAnnotations.Schema.Column("className")]
        [BsonElement("className")]
        public string name { get; set; }

        public static List<UserClass> NewClasses(int startId, int count = 1)
        {
            return Enumerable.Range(startId, count).Select(id => new UserClass { id = id, name = "class" + id }).ToList();
        }
    }


    public class DataSource
    {
        public static void WaitForUpdate() { }

        public static DbContext CreateDbContextForWriting(bool autoInit = true) => CreateDbContext(autoInit);
        public static DbContext CreateDbContext(bool autoInit = true)
        {
            var dbConfig = new DbConfig(Appsettings.json.GetByPath<Dictionary<string, object>>("Vitorm.MongoDB"));
            var dbContext = new Vitorm.MongoDB.DbContext(dbConfig);

            //dbContext.BeginTransaction();

            if (autoInit)
                InitDbContext(dbContext);

            return dbContext;
        }


        public static void InitDbContext(DbContext dbContext)
        {
            #region #1 init User
            {
                dbContext.TryDropTable<User>();
                dbContext.TryCreateTable<User>();

                var users = new List<User> {
                    new User { id=1, name="u146", fatherId=4, motherId=6 },
                    new User { id=2, name="u246", fatherId=4, motherId=6 },
                    new User { id=3, name="u356", fatherId=5, motherId=6 },
                    new User { id=4, name="u400" },
                    new User { id=5, name="u500" },
                    new User { id=6, name="u600" },
                };
                users.ForEach(user =>
                {
                    user.birth = DateTime.Parse("2021-01-01 00:00:00").AddHours(user.id);
                    user.classId = user.id % 2 + 1;
                });

                dbContext.AddRange(users);
            }
            #endregion

            #region #2 init Class
            {
                dbContext.TryDropTable<UserClass>();
                dbContext.TryCreateTable<UserClass>();
                dbContext.AddRange(UserClass.NewClasses(1, 6));
            }
            #endregion
        }

    }
}
