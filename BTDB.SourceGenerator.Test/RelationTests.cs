using System.Threading.Tasks;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class RelationTests : GeneratorTestsBase
{
    [Fact]
    public Task VerifyBasicRelation()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)] public int ParentId { get; set; }
                [PrimaryKey(2)] public int PersonId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IPersonTable : ICovariantRelation<Person>
            {
            }
            """);
    }

    [Fact]
    public Task VerifyRelationWithSecondaryKey()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)] public int ParentId { get; set; }
                [PrimaryKey(2)] [SecondaryKey("PersonId", Order = 1) public int PersonId { get; set; }
                [PrimaryKey(3, true)] public string Name { get; set; } = null!;
                [SecondaryKey("LowerCaseName", IncludePrimaryKeyOrder = 1, Order = 2)] public string LowerCaseName => Name.ToLower();
                [InKeyValue(4)] public string Description { get; set; } = null!;
            }

            public interface IPersonTable : IRelation<Person>
            {
            }
            """);
    }

    [Fact]
    public Task VerifyCannotUsePrimaryKeyTogetherWithInKeyValue()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)]
                [InKeyValue(2)] public int Id { get; set; }
            }

            public interface IPersonTable : IRelation<Person>
            {
            }

            """);
    }

    [Fact]
    public Task VerifyThatSecondaryKeyCannotBeNamedId()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)] public int Id { get; set; }
                [SecondaryKey("Id")] public string Name { get; set; } = null!;
            }

            public interface IPersonTable : IRelation<Person>
            {
            }

            """);
    }

    [Fact]
    public Task VerifyThatInKeyValueCannotBeAlsoSecondaryKey()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)]
                public int Id { get; set; }
                [SecondaryKey("Name")]
                [InKeyValue(2)]
                public string Name { get; set; } = null!;
            }

            public interface IPersonTable : IRelation<Person>
            {
            }

            """);
    }

    [Fact]
    public Task VerifyThatInKeyValueCannotBeBeforePrimaryKey()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [InKeyValue(1)]
                public int InKeyValue { get; set; }
                [PrimaryKey(2)]
                public int PrimaryKey { get; set; }
            }
            public interface IPersonTable : IRelation<Person>
            {
            }

            """);
    }

    [Fact]
    public Task VerifyThatTwoPrimaryKeysCannotHaveSameOrder()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)]
                public int P1 { get; set; }
                [PrimaryKey(1)]
                public int P1Also { get; set; }
            }
            public interface IPersonTable : IRelation<Person>
            {
            }

            """);
    }

    [Fact]
    public Task VerifyThatTwoSecondaryKeysCannotHaveSameOrder()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)]
                public int P1 { get; set; }
                [SecondaryKey("SK", Order = 1)]
                public int S1 { get; set; }
                [SecondaryKey("SK", Order = 1)]
                public int S1Also { get; set; }
            }
            public interface IPersonTable : IRelation<Person>
            {
            }

            """);
    }

    [Fact]
    public Task VerifyThatIEnumerableIsOk()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
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
                IEnumerable<UserNotice> ListByNoticeId(AdvancedEnumeratorParam<ulong> noticeId);
            }

            """);
    }

    [Fact]
    public Task VerifyThatIEnumeratorIsError()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
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
                IEnumerator<UserNotice> ListByNoticeId(AdvancedEnumeratorParam<ulong> noticeId);
            }

            """);
    }
}
