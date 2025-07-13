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

    [Fact]
    public Task ComplexSkymambaExample()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            public class ContinentMigrationInfo
            {
                [PrimaryKey(1)]
                public ulong CompanyId { get; set; }
            }

            public interface ICompanyTableBase<T> : ICovariantCompanyTableBase<T>, IRelation<T>
                where T : class
            {
            }

            public interface ICovariantCompanyTableBase<out T> : ICovariantRelation<T> where T : class
            {
            }

            public interface IPeripheryMigrationInfoTable : ICompanyTableBase<ContinentMigrationInfo>
            {
            }

            """);
    }

    [Fact]
    public Task NameOfSecondaryKeyCouldBeNameofExpression()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            public class Item : ICompanyRecord
            {
                [PrimaryKey(1)]
                public ulong CompanyId { get; set; }

                [PrimaryKey(2)]
                public string Queue { get; set; }

                [PrimaryKey(3)]
                public System.Guid ItemId { get; set; }

                [SecondaryKey(nameof(LockDeadline), IncludePrimaryKeyOrder = 2, Order = 3)]
                public int Priority { get; set; }

                /// <summary>
                /// The deadline by which the worker should renew lock or complete the work;
                /// after deadline work item is available to other worker.
                /// </summary>
                [SecondaryKey(nameof(LockDeadline), Order = 4)]
                public System.DateTime LockDeadline { get; set; }
            }


            public interface IItemTable : IRelation<Item>
            {
            }

            """);
    }

    [Fact]
    public Task VerifyItAutomaticallyGeneratesMetadataForMemberTypes()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            public class DeliveryRuleV1
            {
                public DeliveryRuleV1()
                {
                    Status = 100;
                }

                public IList<Activity> Activities { get; set; }

                [PrimaryKey(1)] public ulong Id { get; set; }

                public int Status { get; set; }
            }

            public interface IDeliveryRuleTable : IRelation<DeliveryRuleV1>
            {
                void Insert(DeliveryRuleV1 job);
            }

            public class Activity
            {
                public ulong Id { get; set; }
            }
            """);
    }

    [Fact]
    public Task VerifyVariantsAreRegistered()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;
            using System.Collections.Generic;

            public class Test
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                public string Name { get; set; }
                public int Age { get; set; }
            }

            public class JustAge
            {
                public int Age { get; set; }
            }

            public class JustName
            {
                public string Name { get; set; }
            }

            public interface IVariantTestTable : IRelation<Test>
            {
                JustAge FindById(ulong id);
                ulong GatherById(ICollection<JustName> items, long skip, long take, Constraint<ulong> id);
            }
            """);
    }

    [Fact]
    public Task VerifyThatOnSerializeAttributeOnStaticMethodShowsError()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Test
            {
                [PrimaryKey(1)] public ulong Id { get; set; }

                [OnSerialize]
                public static void Serialize()
                {
                }
            }

            public interface ITestTable : IRelation<Test>
            {
            }
            """);
    }

    [Fact]
    public Task VerifyThatOnSerializeAttributeOnNonVoidMethodShowsError()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Test
            {
                [PrimaryKey(1)] public ulong Id { get; set; }

                [OnSerialize]
                public int Serialize()
                {
                    return 42;
                }
            }

            public interface ITestTable : IRelation<Test>
            {
            }
            """);
    }

    [Fact]
    public Task VerifyThatOnSerializeAttributeOnMethodWithParametersShowsError()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Test
            {
                [PrimaryKey(1)] public ulong Id { get; set; }

                [OnSerialize]
                public void Serialize(int a)
                {
                }
            }

            public interface ITestTable : IRelation<Test>
            {
            }
            """);
    }

    [Fact]
    public Task VerifyThatOnSerializeAttributeIsProperlyGenerated()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Test
            {
                [PrimaryKey(1)] public ulong Id { get; set; }

                [OnSerialize]
                public void MethodA()
                {
                }

                [OnSerialize]
                void MethodB()
                {
                }
            }

            public interface ITestTable : IRelation<Test>
            {
            }
            """);
    }
}
