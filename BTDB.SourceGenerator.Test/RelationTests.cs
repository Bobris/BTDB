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
    public Task VerifyRelationWithoutPrimaryKey()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
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

    [Fact]
    public Task VerifyThatOnBeforeRemoveAttributeOnStaticMethodShowsError()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Test
            {
                [PrimaryKey(1)] public ulong Id { get; set; }

                [OnBeforeRemove]
                static public void MethodA()
                {
                }
            }

            public interface ITestTable : IRelation<Test>
            {
            }
            """);
    }

    [Fact]
    public Task VerifyThatOnBeforeRemoveAttributeOnNonVoidOrBoolMethodShowsError()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Test
            {
                [PrimaryKey(1)] public ulong Id { get; set; }

                [OnBeforeRemove]
                public int MethodA()
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
    public Task VerifyComplexOnBeforeRemove()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;
            public interface I3rdPartyInterface { string Name { get; } }

            public class Person
            {
                [PrimaryKey(1)] public string Name { get; set; }

                [OnBeforeRemove]
                public bool OnBeforeRemove()
                {
                    return true;
                }

                [OnBeforeRemove]
                public bool SecondOnBeforeRemove(IObjectDBTransaction transaction)
                {
                    return true;
                }

                [OnBeforeRemove]
                public bool ThirdOnBeforeRemove(I3rdPartyInterface dependency, I3rdPartyInterface? key1)
                {
                    return true;
                }
            }

            public interface IPersonTable : IRelation<Person>
            {
            }
            """);
    }

    [Fact]
    public Task VerifyComplexPrivateOnBeforeRemove()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;
            public interface I3rdPartyInterface { string Name { get; } }

            public class Person
            {
                [PrimaryKey(1)] public string Name { get; set; }

                [OnBeforeRemove]
                bool OnBeforeRemove()
                {
                    return true;
                }

                [OnBeforeRemove]
                bool SecondOnBeforeRemove(IObjectDBTransaction transaction)
                {
                    return true;
                }

                [OnBeforeRemove]
                void ThirdOnBeforeRemove(I3rdPartyInterface dependency, I3rdPartyInterface? key1)
                {
                }
            }

            public interface IPersonTable : IRelation<Person>
            {
            }
            """);
    }

    [Fact]
    public Task VerifyInheritanceFromGenericInterface()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Person
            {
                [PrimaryKey(1)] public string Name { get; set; }
            }

            public interface IWithInsert<T>
            {
                void Insert(T user);
            }

            public interface IPersonTable : IWithInsert<Person>, IRelation<Person>
            {
            }
            """);
    }

    [Fact]
    public Task VerifyErrorIsShownForWrongNumberOfParameters()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Person
            {
                [PrimaryKey(1)] public string Name { get; set; }
            }

            public interface IPersonTable : IRelation<Person>
            {
                void Insert();
            }
            """);
    }

    [Fact]
    public Task VerifyErrorIsShownForWrongReturnType()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Person
            {
                [PrimaryKey(1)] public string Name { get; set; }
            }

            public interface IPersonTable : IRelation<Person>
            {
                int Insert(Person person);
            }
            """);
    }

    [Fact]
    public Task FindByMethodsChecksParameterTypes()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System;
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            public class ProductionTrackingDaily
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public DateTime ProductionDate { get; set; }
            }

            public interface IProductionInvalidTable : IRelation<ProductionTrackingDaily>
            {
                void Insert(ProductionTrackingDaily productionTrackingDaily);
                IEnumerable<ProductionTrackingDaily> FindByProductionDateWithCompanyId(ulong companyId,
                    AdvancedEnumeratorParam<DateTime> productionDate);
            }
            """);
    }

    [Fact]
    public Task FindByMethodChecksParameterCount()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System;
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                void Insert(Product product);
                // FindById with missing parameter - should return single item but only has 1 of 2 params
                Product FindById(ulong companyId);
            }
            """);
    }

    [Fact]
    public Task GatherByMethodMustReturnUlong()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // GatherById must return ulong, not int
                int GatherById(ICollection<Product> items, long skip, long take);
            }
            """);
    }

    [Fact]
    public Task GatherByMethodMustHaveAtLeastThreeParameters()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // GatherById must have at least 3 parameters (collection, skip, take)
                ulong GatherById(ICollection<Product> items);
            }
            """);
    }

    [Fact]
    public Task GatherByMethodFirstParameterMustBeICollection()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // First parameter must implement ICollection<>
                ulong GatherById(IEnumerable<Product> items, long skip, long take);
            }
            """);
    }

    [Fact]
    public Task GatherByMethodSecondParameterMustBeNamedSkip()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // Second parameter must be named "skip" and be long
                ulong GatherById(ICollection<Product> items, long wrongName, long take);
            }
            """);
    }

    [Fact]
    public Task GatherByMethodThirdParameterMustBeNamedTake()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // Third parameter must be named "take" and be long
                ulong GatherById(ICollection<Product> items, long skip, long wrongName);
            }
            """);
    }
}
