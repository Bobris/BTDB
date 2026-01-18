using System.Threading.Tasks;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class RelationDuplicateGetEnumeratorTests : GeneratorTestsBase
{
    [Fact]
    public Task RelationWithDiamondInheritanceDoesNotDuplicateGetEnumerator()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Widget
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ItemId { get; set; }
            }

            public interface IEnumerableLike<T> where T : class
            {
                IEnumerator<T> GetEnumerator();
            }

            public interface IEnumerableLikeChild<T> : IEnumerableLike<T> where T : class
            {
            }

            public interface IEnumerableLikeChild2<T> : IEnumerableLike<T> where T : class
            {
            }

            public interface ICompanyItemTableBase<T> : IEnumerableLikeChild<T>, IEnumerableLikeChild2<T>, IRelation<T>
                where T : class
            {
            }

            public interface IWidgetTable : ICompanyItemTableBase<Widget>
            {
            }
            """);
    }
}
