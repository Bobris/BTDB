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
    public Task VerifyThatIEnumerableIsOkWithCompatibleAdvancedEnumeratorParam()
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
                IEnumerable<UserNotice> ListByNoticeId(AdvancedEnumeratorParam<uint> noticeId);
            }

            """);
    }

    [Fact]
    public Task ReportErrorForSuperfluousMethodParameter()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            public class Person
            {
                [PrimaryKey(1)] public ulong TenantId { get; set; }
                [PrimaryKey(2)] public ulong Id { get; set; }
                [SecondaryKey("Name", IncludePrimaryKeyOrder = 1)] public string Name { get; set; } = null!;
            }

            public interface IPersonTableSuperfluousParameter : IRelation<Person>
            {
                void Insert(Person person);
                // SecondaryKey("Name") includes primary key fields at the end; validation trims that tail.
                IEnumerable<Person> ListByName(ulong tenantId, string name, AdvancedEnumeratorParam<uint> param);
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
    public Task ReportsProblemAboutUsageOfUnknownMethod()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Person
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
            }

            public interface IWronglyDefinedUnknownMethod : IRelation<Person>
            {
                void Insert(Person person);
                void Delete(Person person);
            }

            """);
    }

    [Fact]
    public Task ListByMethodMustReturnEnumerable()
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
                UserNotice ListByNoticeId(ulong noticeId);
            }

            """);
    }

    [Fact]
    public Task ListByMethodWithAdvancedEnumeratorParamWrongType()
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
                IEnumerable<UserNotice> ListByNoticeId(AdvancedEnumeratorParam<int> noticeId);
            }

            """);
    }

    [Fact]
    public Task ListByMethodWithAdvancedEnumeratorParamTooManyParameters()
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
                IEnumerable<UserNotice> ListByNoticeId(ulong noticeId, ulong userId, AdvancedEnumeratorParam<ulong> noticeIdParam);
            }

            """);
    }

    [Fact]
    public Task ListByMethodUsesExplicitSecondaryKeyPrefix()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            public class ContentVersion
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }

                [PrimaryKey(2)] public ulong ContentId { get; set; }

                [PrimaryKey(3)]
                [SecondaryKey("State", Order = 4)]
                public uint Version { get; set; }

                [SecondaryKey("State", Order = 3, IncludePrimaryKeyOrder = 2)]
                public ContentVersionState State { get; set; }
            }

            public enum ContentVersionState
            {
                Published = 0,
                PreviouslyPublished = 1,
            }

            public interface IContentVersionTable : IRelation<ContentVersion>
            {
                IEnumerable<ContentVersion> ListByState(ulong companyId, ulong contentId, ContentVersionState state,
                    AdvancedEnumeratorParam<uint> param);
            }

            """);
    }

    [Fact]
    public Task ConstraintMethodsAllowImplicitPrimaryKeyFields()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Collections.Generic;
            using BTDB.ODBLayer;

            public class ThingWithSK
            {
                public ThingWithSK(ulong tenant, string name, uint age)
                {
                    Tenant = tenant;
                    Name = name;
                    Age = age;
                }

                [PrimaryKey(1)] public ulong Tenant { get; set; }

                [PrimaryKey(2)]
                [SecondaryKey("Name", IncludePrimaryKeyOrder = 0, Order = 1)]
                public string Name { get; set; }

                [SecondaryKey("Name", IncludePrimaryKeyOrder = 0, Order = 2)]
                public uint Age { get; set; }
            }

            public interface IThingWithSKTable : IRelation<ThingWithSK>
            {
                IEnumerable<ThingWithSK> ScanByName(Constraint<string> name, Constraint<ulong> age,
                    Constraint<ulong> tenant);

                ulong GatherByName(List<ThingWithSK> target, long skip, long take, Constraint<string> name,
                    Constraint<ulong> age, IOrderer[] orderers);

                ThingWithSK? FirstByNameOrDefault(Constraint<string> name, Constraint<ulong> age, IOrderer[] orderers);

                ThingWithSK FirstByName(Constraint<string> name, Constraint<ulong> age, IOrderer[] orderers);
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
    public Task GatherBySecondaryKeyUsesSecondaryKeyIndex()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;
            using System.Collections.Generic;

            public class Person
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                [SecondaryKey("Email")] public string Email { get; set; } = null!;
            }

            public interface IEmailTable : IRelation<Person>
            {
                ulong GatherByEmail(ICollection<Person> items, long skip, long take, Constraint<string> email);
            }
            """);
    }

    [Fact]
    public Task ScanBySecondaryKeyUsesSecondaryKeyIndex()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Person
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                [SecondaryKey("Email")] public string Email { get; set; } = null!;
            }

            public interface IEmailTable : IRelation<Person>
            {
                System.Collections.Generic.IEnumerable<Person> ScanByEmail(Constraint<string> email);
            }
            """);
    }

    [Fact]
    public Task ScanByPrimaryKeyUsesPrimaryKeyPrefix()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Person
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IPersonTable : IRelation<Person>
            {
                System.Collections.Generic.IEnumerable<Person> ScanById(Constraint<ulong> id);
            }
            """);
    }

    [Fact]
    public Task FirstByPrimaryKeyUsesPrimaryKeyIndex()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong Id { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                Product FirstByIdOrDefault(Constraint<ulong> id);
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
    public Task FindByMethodMustReturnClassOrEnumerable()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
            }

            public interface IProductTable : IRelation<Product>
            {
                int FindById(ulong companyId);
            }
            """);
    }

    [Fact]
    public Task FindByMethodMustReturnSerializableClass()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System.Threading.Tasks;
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
            }

            public interface IProductTable : IRelation<Product>
            {
                Task FindById(ulong companyId);
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

    [Fact]
    public Task FirstByMethodMustHaveClassReturnType()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // FirstById must return class type, not struct
                ulong FirstById(Constraint<ulong> companyId);
            }
            """);
    }

    [Fact]
    public Task FirstByMethodWithTooManyConstraintParameters()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // FirstById has too many constraint parameters (3) for a 2-field primary key
                Product FirstById(Constraint<ulong> companyId, Constraint<ulong> productId, Constraint<string> name);
            }
            """);
    }

    [Fact]
    public Task FirstByMethodWithConstraintParameterTypeMismatch()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // FirstById has wrong constraint type - should be Constraint<ulong> not Constraint<string>
                Product FirstById(Constraint<string> companyId);
            }
            """);
    }

    [Fact]
    public Task FirstByMethodWithConstraintParameterNameMismatch()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // FirstById has wrong parameter name - should be "companyId" not "wrongName"
                Product FirstById(Constraint<ulong> wrongName);
            }
            """);
    }

    [Fact]
    public Task FirstByMethodWithNonConstraintParameter()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // FirstById parameter must be Constraint<T>, not raw type
                Product FirstById(ulong companyId);
            }
            """);
    }

    [Fact]
    public Task ContainsMethodMustReturnBool()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // Contains must return bool, not int
                int Contains(ulong companyId, ulong productId);
            }
            """);
    }

    [Fact]
    public Task ContainsMethodWithWrongParameterType()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // Contains parameter type must match primary key type
                bool Contains(string companyId, ulong productId);
            }
            """);
    }

    [Fact]
    public Task ContainsMethodWithWrongParameterName()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // Contains parameter name must match primary key field name
                bool Contains(ulong wrongName, ulong productId);
            }
            """);
    }

    [Fact]
    public Task ContainsMethodWithMissingParameter()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // Contains must specify all primary key fields
                bool Contains(ulong companyId);
            }
            """);
    }

    [Fact]
    public Task FloatIndexIsRejected()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class FloatIndexEntity
            {
                [PrimaryKey(1)] public float Id { get; set; }
            }

            public interface IFloatIndexTable : IRelation<FloatIndexEntity>
            {
                bool Contains(float id);
            }
            """);
    }

    [Fact]
    public Task ContainsSupportsMoreKeyTypes()
    {
        // language=cs
        return VerifySourceGenerator("""
            using System;
            using System.Collections.Generic;
            using System.Net;
            using Microsoft.Extensions.Primitives;
            using BTDB.ODBLayer;

            public class ContainsKeyTypes
            {
                [PrimaryKey(1)] public string Name { get; set; } = "";
                [PrimaryKey(2)] public TimeSpan Duration { get; set; }
                [PrimaryKey(3)] public IPAddress Address { get; set; } = IPAddress.Loopback;
                [PrimaryKey(4)] public Version ApiVersion { get; set; } = new Version(1, 0);
                [PrimaryKey(5)] public StringValues Tags { get; set; } = StringValues.Empty;
                [PrimaryKey(6)] public List<string> Names { get; set; } = new List<string>();
                [PrimaryKey(7)] public List<ulong> Counters { get; set; } = new List<ulong>();
            }

            public interface IContainsKeyTypesTable : IRelation<ContainsKeyTypes>
            {
                bool Contains(string name, TimeSpan duration, IPAddress address, Version apiVersion,
                    StringValues tags, List<string> names, List<ulong> counters);
            }
            """);
    }

    [Fact]
    public Task ContainsMethodGeneratesKeyLookup()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class ProductionTrackingDaily
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public global::System.DateTime ProductionDate { get; set; }
            }

            public interface IProductionTableWithContains : IRelation<ProductionTrackingDaily>
            {
                bool Contains(ulong companyId, global::System.DateTime productionDate);
            }
            """);
    }

    [Fact]
    public Task AnyByMethodMustReturnBool()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // AnyById must return bool, not int
                int AnyById(ulong companyId);
            }
            """);
    }

    [Fact]
    public Task AnyByMethodWithWrongParameterType()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // AnyById parameter type must match primary key type
                bool AnyById(string companyId);
            }
            """);
    }

    [Fact]
    public Task AnyByMethodWithWrongParameterName()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // AnyById parameter name must match primary key field name
                bool AnyById(ulong wrongName);
            }
            """);
    }

    [Fact]
    public Task AnyByMethodWithAdvancedEnumeratorParamWrongType()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // AdvancedEnumeratorParam generic type must match last field type (ProductId is ulong, not int)
                bool AnyById(ulong companyId, AdvancedEnumeratorParam<int> productIdParam);
            }
            """);
    }

    [Fact]
    public Task AnyByMethodWithAdvancedEnumeratorParamValid()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // Valid: AdvancedEnumeratorParam<ulong> matches ProductId type
                bool AnyById(ulong companyId, AdvancedEnumeratorParam<ulong> productIdParam);
            }
            """);
    }

    [Fact]
    public Task CountByMethodMustReturnLongLikeType()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // CountById must return int, uint, long, or ulong, not string
                string CountById(ulong companyId);
            }
            """);
    }

    [Fact]
    public Task CountByMethodWithWrongParameterType()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // CountById parameter type must match primary key type
                int CountById(string companyId);
            }
            """);
    }

    [Fact]
    public Task CountByMethodWithWrongParameterName()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // CountById parameter name must match primary key field name
                long CountById(ulong wrongName);
            }
            """);
    }

    [Fact]
    public Task CountByMethodWithAdvancedEnumeratorParamWrongType()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // AdvancedEnumeratorParam generic type must match last field type (ProductId is ulong, not string)
                long CountById(ulong companyId, AdvancedEnumeratorParam<string> productIdParam);
            }
            """);
    }

    [Fact]
    public Task CountByMethodWithAdvancedEnumeratorParamValid()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // Valid: AdvancedEnumeratorParam<ulong> matches ProductId type
                long CountById(ulong companyId, AdvancedEnumeratorParam<ulong> productIdParam);
            }
            """);
    }

    [Fact]
    public Task RemoveByMethodMustReturnLongLikeType()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // RemoveById must return int, uint, long, or ulong, not string
                string RemoveById(ulong companyId, ulong productId);
            }
            """);
    }

    [Fact]
    public Task RemoveByMethodWithWrongParameterType()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // RemoveById parameter type must match primary key type
                int RemoveById(string companyId, ulong productId);
            }
            """);
    }

    [Fact]
    public Task RemoveByMethodWithWrongParameterName()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // RemoveById parameter name must match primary key field name
                long RemoveById(ulong wrongName, ulong productId);
            }
            """);
    }

    [Fact]
    public Task RemoveBySecondaryKeyIsUnsupported()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class ContactGroupRelation
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong GroupId { get; set; }

                [PrimaryKey(3)]
                [SecondaryKey("ContactId", IncludePrimaryKeyOrder = 1)]
                public ulong ContactId { get; set; }
            }

            public interface IContactGroupRelationTable : IRelation<ContactGroupRelation>
            {
                int RemoveByContactId(ulong companyId, ulong contactId);
            }
            """);
    }

    [Fact]
    public Task RemoveByMethodWithAdvancedEnumeratorParamWrongType()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // AdvancedEnumeratorParam generic type must match last field type (ProductId is ulong, not int)
                int RemoveById(ulong companyId, AdvancedEnumeratorParam<int> productIdParam);
            }
            """);
    }

    [Fact]
    public Task RemoveByMethodWithAdvancedEnumeratorParamValid()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                // Valid: AdvancedEnumeratorParam<ulong> matches ProductId type
                int RemoveById(ulong companyId, AdvancedEnumeratorParam<ulong> productIdParam);
            }
            """);
    }

    [Fact]
    public Task RemoveByMethodGeneratesRemoveById()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                void RemoveById(ulong companyId, ulong productId);
                bool RemoveByIdOrDefault(ulong companyId, ulong productId);
            }
            """);
    }

    [Fact]
    public Task RemoveByIdPartialGeneratesPrefixPartial()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Product
            {
                [PrimaryKey(1)] public ulong CompanyId { get; set; }
                [PrimaryKey(2)] public ulong ProductId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IProductTable : IRelation<Product>
            {
                int RemoveByIdPartial(ulong companyId, int maxCount);
            }
            """);
    }

    [Fact]
    public Task RemoveByIdPrefixUsesFastRemove()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class DataSamePrefix
            {
                [PrimaryKey(1)]
                [SecondaryKey("S", Order = 1)]
                public int A { get; set; }

                [PrimaryKey(2)] public int B { get; set; }

                [PrimaryKey(3)]
                [SecondaryKey("S", Order = 2)]
                public int C { get; set; }
            }

            public interface IDataSamePrefixTable : IRelation<DataSamePrefix>
            {
                int RemoveById(int a);
            }
            """);
    }

    [Fact]
    public Task RemoveWithSizesByIdGeneratesPrimaryKeyConstraints()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Document
            {
                [PrimaryKey(1)] public ulong TenantId { get; set; }
                [PrimaryKey(2)] public string Key { get; set; } = null!;
                public string Value { get; set; } = null!;
            }

            public interface IDocumentTable : IRelation<Document>
            {
                (ulong Count, ulong KeySizes, ulong ValueSizes) RemoveWithSizesById(Constraint<ulong> tenantId,
                    Constraint<string> key);
            }
            """);
    }

    [Fact]
    public Task UpdateMethodsGenerateBaseCalls()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Person
            {
                [PrimaryKey] public ulong Id { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IPersonTable : IRelation<Person>
            {
                void Update(Person person);
                void ShallowUpdate(Person person);
            }
            """);
    }

    [Fact]
    public Task UpdateByIdGeneratesValueUpdates()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.Encrypted;
            using BTDB.ODBLayer;

            public class Person
            {
                [PrimaryKey(1)] public ulong TenantId { get; set; }
                [PrimaryKey(2)] public ulong Id { get; set; }
                public string Name { get; set; } = null!;
                public EncryptedString Secret { get; set; }
            }

            public interface IPersonTable : IRelation<Person>
            {
                bool UpdateById(ulong tenantId, ulong id);
                bool UpdateById(ulong tenantId, ulong id, string name);
                void UpdateByIdSecret(ulong tenantId, ulong id, string secret);
            }
            """);
    }

    [Fact]
    public Task DefaultInterfaceMethodIsNotGenerated()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Item
            {
                [PrimaryKey] public ulong Id { get; set; }
            }

            public interface IItemTable : IRelation<Item>
            {
                int CountN1Groups() => 42;
                void Update(Item item);
            }
            """);
    }
}
