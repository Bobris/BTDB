using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Sample3rdPartyLib;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// <summary>
/// Reproduces MissingMethodException when storing objects whose keys inherit
/// auto-properties from a generic base class defined in an external assembly.
///
/// Root cause: The source generator cannot detect auto-properties from external
/// assemblies (DeclaringSyntaxReferences is empty), so it generates
/// UnsafeAccessor(Method, Name="get_Id") instead of
/// UnsafeAccessor(Field, Name="&lt;Id&gt;k__BackingField").
/// </summary>
public class ObjectDbExternalGenericBaseTest : ObjectDbTestBase
{
    public ObjectDbExternalGenericBaseTest(ITestOutputHelper output) : base(output)
    {
    }

    // Derived class in current assembly inheriting auto-properties from
    // generic base in external assembly (Sample3rdPartyLib)
    [BTDB.Generate]
    public class ConcreteKey : SampleGenericBase<ulong>
    {
        public ConcreteKey()
        {
        }

        public ConcreteKey(ulong companyId, ulong id) : base(companyId, id)
        {
        }
    }

    public class ItemValue
    {
        public string Name { get; set; }
    }

    public class EntityWithDictionary
    {
        [PrimaryKey(1)] public ulong TenantId { get; set; }
        public IDictionary<ConcreteKey, ItemValue> Items { get; set; }
    }

    public interface IEntityWithDictionaryTable : IRelation<EntityWithDictionary>
    {
    }

    [Fact]
    public void CanStoreObjectWithKeyInheritingFromExternalGenericBase()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IEntityWithDictionaryTable>();

        var entity = new EntityWithDictionary
        {
            TenantId = 1,
            Items = new Dictionary<ConcreteKey, ItemValue>
            {
                {
                    new ConcreteKey(42, 100),
                    new ItemValue { Name = "TestItem" }
                }
            }
        };

        table.Upsert(entity);

        var loaded = table.First();
        Assert.Equal(1ul, loaded.TenantId);

        var key = new ConcreteKey(42, 100);
        Assert.True(loaded.Items.ContainsKey(key));
        Assert.Equal("TestItem", loaded.Items[key].Name);
    }
}
