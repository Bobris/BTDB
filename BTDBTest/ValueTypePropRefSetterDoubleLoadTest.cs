using System;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// <summary>
/// Reproduces a bug where value-type fields loaded via PropRefSetter (not ByteOffset)
/// that fit in Int128 get two loaders registered — the FitsInInt128 loader AND the
/// stackAllocator loader — because of a missing `continue` after the FitsInInt128 block
/// in RelationInfo.ItemLoaderInfo.CreateLoader. The double-read corrupts the MemReader
/// position and causes NullReferenceException or wrong data on subsequent fields.
///
/// In real code this happens when an entity inherits a value-type auto-property from
/// a base class in a different assembly: the source generator cannot UnsafeAccessor the
/// backing field, so it emits PropRefSetter instead of ByteOffset.
/// </summary>
public class ValueTypePropRefSetterDoubleLoadTest : ObjectDbTestBase
{
    public ValueTypePropRefSetterDoubleLoadTest(ITestOutputHelper output) : base(output)
    {
    }

    // --- Entity with explicit getter/setter for a value-type property ---
    // Using block-body getter so ExtractPropertyFromGetter returns null
    // and the source generator emits PropRefSetter instead of ByteOffset.

    [Generate]
    [GenerateFor(typeof(IExplicitDateTable))]
    public class EntityWithExplicitDate
    {
        [PrimaryKey(1)]
        public ulong Id { get; set; }

        public string Name { get; set; } = "";

        // Explicit getter/setter → PropRefSetter in generated metadata
        DateTime _uploadDate;
        public DateTime UploadDate
        {
            get { return _uploadDate; }
            set { _uploadDate = value; }
        }

        // Field AFTER UploadDate — will be corrupted by double-read of DateTime
        public int TrailingValue { get; set; }
    }

    public interface IExplicitDateTable : IRelation<EntityWithExplicitDate>
    {
        EntityWithExplicitDate? FindByIdOrDefault(ulong id);
    }

    [Fact]
    public void ValueTypePropRefSetterFieldRoundTrips()
    {
        var date = new DateTime(2025, 3, 16, 12, 0, 0, DateTimeKind.Utc);

        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IExplicitDateTable>();
            table.Upsert(new EntityWithExplicitDate
            {
                Id = 1,
                Name = "test",
                UploadDate = date,
                TrailingValue = 42
            });
            tr.Commit();
        }

        using (var tr = _db.StartReadOnlyTransaction())
        {
            var table = tr.GetRelation<IExplicitDateTable>();
            var item = table.FindByIdOrDefault(1);
            Assert.NotNull(item);
            Assert.Equal((ulong)1, item.Id);
            Assert.Equal("test", item.Name);
            Assert.Equal(date, item.UploadDate);
            Assert.Equal(42, item.TrailingValue);
        }
    }
}
