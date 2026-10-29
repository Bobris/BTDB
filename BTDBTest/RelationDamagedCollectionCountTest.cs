using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// A damaged count of a stored list or dictionary used to size the collection: past int.MaxValue it was a negative
/// capacity and reading failed with ArgumentOutOfRangeException. The count only hints the capacity now, so the read
/// fails at the end of the value instead.
public class RelationDamagedCollectionCountTest : ObjectDbTestBase
{
    public RelationDamagedCollectionCountTest(ITestOutputHelper output) : base(output)
    {
    }

    public class Row
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public List<int>? Items { get; set; }
        public Dictionary<int, int>? Map { get; set; }
    }

    public interface IRowTable : IRelation<Row>
    {
        void Insert(Row row);
        Row? FindById(ulong id);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DamagedCollectionCountFailsAsDamage(bool noEmit)
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = noEmit;
            ObjectDB.ResetAllMetadataCaches();
            ReopenDb();
            CheckDamagedCounts();
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    void CheckDamagedCounts()
    {
        byte[] prefix;
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IRowTable>();
            table.Insert(new()
            {
                Id = 1, Items = [100, 100, 100, 100, 100], Map = new() { [1] = 2, [3] = 4, [5] = 6, [7] = 8, [9] = 10 }
            });
            prefix = ((IRelationDbManipulator)table).RelationInfo.Prefix;
            tr.Commit();
        }

        byte[] original;
        using (var tr = _lowDb.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            Assert.True(cursor.FindFirstKey(prefix));
            var buffer = Span<byte>.Empty;
            original = cursor.GetValueSpan(ref buffer).ToArray();
        }

        var huge = new MemWriter();
        huge.WriteVUInt32((1u << 31) + 1);
        var hugeBytes = huge.GetSpan().ToArray();
        var unexpected = new List<string>();
        // Both counts are 5; replacing each byte of 5 in turn damages each of them.
        for (var at = 0; at < original.Length; at++)
        {
            if (original[at] != 5) continue;
            using (var tr = _lowDb.StartTransaction())
            {
                using var cursor = tr.CreateCursor();
                cursor.FindFirstKey(prefix);
                cursor.SetValue(original[..at].Concat(hugeBytes).Concat(original[(at + 1)..]).ToArray());
                tr.Commit();
            }

            try
            {
                using var readTr = _db.StartReadOnlyTransaction();
                readTr.GetRelation<IRowTable>().FindById(1);
            }
            catch (Exception error) when (error is EndOfStreamException or BTDBException or InvalidDataException)
            {
            }
            catch (Exception error)
            {
                unexpected.Add($"at {at}: {error.GetType().Name}: {error.Message}");
            }
        }

        Assert.True(unexpected.Count == 0, string.Join("\n", unexpected));
    }
}
