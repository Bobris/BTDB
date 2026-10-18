using System.Collections.Generic;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class RelationQueryInterceptorCoverageTests : GeneratorTestsBase
{
    const string Source = """
        using System;
        using System.Linq;
        using BTDB.ODBLayer;

        namespace TestNamespace;

        public class Person
        {
            [PrimaryKey(1)] public ulong Id { get; set; }
            public string Name { get; set; } = "";
        }

        public interface IPersonTable : IRelation<Person>
        {
        }

        public static class Queries
        {
            public static int Count(IPersonTable table) => table.Query<Person>().Count(p => p.Id > 1);
        }
        """;

    static CSharpParseOptions OptionsWithFeature(string feature, string value) =>
        CSharpParseOptions.Default.WithFeatures([new KeyValuePair<string, string>(feature, value)]);

    [Fact]
    public void OptInMustNameInterceptorsNamespace()
    {
        Assert.Empty(GetInterceptorSources(Source, OptionsWithFeature("InterceptorsNamespaces", "Other.Namespace")));
    }

    [Fact]
    public void PreviewFeatureNameWithMoreNamespacesIsAccepted()
    {
        Assert.Single(GetInterceptorSources(Source,
            OptionsWithFeature("InterceptorsPreviewNamespaces", "Other.Namespace; BTDB.Interceptors")));
    }

    [Fact]
    public void OnlyQueryableOperatorsWithoutComparersAreIntercepted()
    {
        var sources = GetInterceptorSources("""
            using System;
            using System.Collections.Generic;
            using System.Linq;
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                public string Name { get; set; } = "";
            }

            public interface IPersonTable : IRelation<Person>
            {
            }

            public static class Queries
            {
                public static int Enumerable(IPersonTable table) =>
                    table.Query<Person>().AsEnumerable().Where(p => p.Id > 1).Count();

                public static List<Person> WithComparer(IPersonTable table) =>
                    table.Query<Person>().OrderBy(p => p.Name, StringComparer.Ordinal).ToList();

                public static int NamedArgument(IPersonTable table) =>
                    table.Query<Person>().Count(predicate: p => p.Id > 1);
            }
            """, true);
        Assert.Empty(sources);
    }
}
