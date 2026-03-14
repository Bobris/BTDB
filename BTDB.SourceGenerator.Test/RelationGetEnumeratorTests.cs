using System.Threading.Tasks;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class RelationGetEnumeratorTests : GeneratorTestsBase
{
    [Fact]
    public Task GetEnumeratorMayReturnIEnumerator()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.FieldHandler;
            using BTDB.ODBLayer;

            public class UserNotice
            {
                [PrimaryKey(1)] public ulong UserId { get; set; }

                [PrimaryKey(2)]
                [SecondaryKey("NoticeId")]
                public ulong NoticeId { get; set; }
            }

            [PersistedName("UserNotice")]
            public interface IUserNoticeTable : IRelation<UserNotice>
            {
                void Insert(UserNotice un);
                IEnumerator<UserNotice> GetEnumerator();
            }

            """);
    }

    [Fact]
    public Task InheritedNonGenericGetEnumeratorFromCovariantRelationIsIgnored()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public interface ICompanyRecord
            {
                ulong CompanyId { get; }
            }

            public class CompanyBackendActionInputIndexer : ICompanyRecord
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public string InputFullName { get; set; } = string.Empty;
                [PrimaryKey(3)] public ulong UniqueKey { get; set; }
                public ulong ActionId { get; set; }
            }

            public interface ICovariantCompanyItemTableBase<out T> : ICovariantRelation<T>
                where T : class, ICompanyRecord
            {
                IEnumerable<T> FindById(ulong companyId);
                int RemoveById(ulong companyId);
            }

            public interface ICompanyItemTableBase<T> : ICovariantCompanyItemTableBase<T>, IRelation<T>
                where T : class, ICompanyRecord
            {
            }

            public interface ICompanyBackendActionInputIndexerTable : ICompanyItemTableBase<CompanyBackendActionInputIndexer>
            {
                void Insert(CompanyBackendActionInputIndexer indexer);
                void Update(CompanyBackendActionInputIndexer inputIndexer);
                CompanyBackendActionInputIndexer? FindByIdOrDefault(ulong companyId, string inputFullName, ulong uniqueKey);
                bool RemoveById(ulong companyId, string inputFullName, ulong uniqueKey);
            }

            """);
    }
}
