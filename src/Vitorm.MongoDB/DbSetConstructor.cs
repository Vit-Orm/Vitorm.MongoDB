using System;
using System.Reflection;

using Vitorm.Entity;

namespace Vitorm.MongoDB
{
    // https://www.mongodb.com/docs/drivers/csharp/current/
    // https://learn.microsoft.com/zh-cn/aspnet/core/tutorials/first-mongo-app?view=aspnetcore-8.0&tabs=visual-studio

    public class DbSetConstructor
    {
        public static IDbSet CreateDbSet(IDbContext dbContext, IEntityDescriptor entityDescriptor)
        {
            return _CreateDbSet.MakeGenericMethod(entityDescriptor.entityType)
                     .Invoke(null, new object[] { dbContext, entityDescriptor }) as IDbSet;
        }

        static readonly MethodInfo _CreateDbSet = new Func<DbContext, IEntityDescriptor, IDbSet>(CreateDbSet<object>)
                   .Method.GetGenericMethodDefinition();
        public static IDbSet<Entity> CreateDbSet<Entity>(DbContext dbContext, IEntityDescriptor entityDescriptor)
        {
            return new DbSet<Entity>(dbContext, entityDescriptor);
        }

    }
}
