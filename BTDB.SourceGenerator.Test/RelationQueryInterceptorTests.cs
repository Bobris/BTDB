using System.Threading.Tasks;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class RelationQueryInterceptorTests : GeneratorTestsBase
{
    const string Source = """
        using System;
        using System.Collections.Generic;
        using System.Linq;
        using System.Linq.Expressions;
        using BTDB;
        using BTDB.ODBLayer;

        namespace TestNamespace;

        public class Person
        {
            [PrimaryKey(1)] public ulong TenantId { get; set; }
            [PrimaryKey(2)] public ulong Id { get; set; }
            public string? Name { get; set; }
            public int Age { get; set; }
        }

        public interface IPersonTable : IRelation<Person>
        {
        }

        public static class Queries
        {
            public static Person? Point(IPersonTable table, ulong id) =>
                table.Query<Person>().Where(p => p.TenantId == 1 && p.Id == id).FirstOrDefault();

            public static int CountAdults(IPersonTable table, int age) =>
                table.Query<Person>().Count(p => p.Age >= age);

            public static List<string?> Page(IPersonTable table, int skip) =>
                table.Query<Person>().Where(p => p.Age > 18).OrderBy(p => p.Name).ThenBy(p => p.Id).Skip(skip).Take(10)
                    .Select(p => p.Name).ToList();

            public static long Sum(IPersonTable table) => table.Query<Person>().Sum(p => (long)p.Age);

            public static Person? WithDefault(IPersonTable table, Person fallback) =>
                table.Query<Person>().FirstOrDefault(p => p.Age > 99, fallback);

            // Not intercepted: a predicate variable, a query variable, an indexed Where and a non-relation source.
            public static int NotIntercepted(IPersonTable table, Expression<Func<Person, bool>> predicate)
            {
                var query = table.Query<Person>();
                return table.Query<Person>().Count(predicate) + query.Count(p => p.Age > 1) +
                       table.Query<Person>().Where((p, i) => i > 1).Count() +
                       new[] { 1, 2 }.AsQueryable().Where(x => x > 1).Count();
            }
        }
        """;

    [Fact]
    public Task VerifyInterceptsRelationQueryChains()
    {
        return VerifyInterceptorGenerator(Source);
    }

    [Fact]
    public void NothingIsGeneratedWithoutOptIn()
    {
        Assert.Empty(GetInterceptorSources(Source, false));
        Assert.Single(GetInterceptorSources(Source, true));
    }
}
