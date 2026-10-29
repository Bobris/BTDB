using System.Collections.Generic;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class LeakDetectionNestedLazyContentTest(ITestOutputHelper output) : ObjectDbTestBase(output)
{
    [Generate]
    public class Holder
    {
        public ILazyUlongList? List { get; set; }
        public IRoaringBitmap? Bitmap { get; set; }
        public IOrderedSet<int>? Set { get; set; }
        public IDictionary<int, int>? Lazy { get; set; }
        public Holder? Next { get; set; }
    }

    public class Row
    {
        [PrimaryKey] public ulong Id { get; set; }
        public Dictionary<int, Holder>? Map { get; set; }
        public List<Holder>? Items { get; set; }
        public Holder? Direct { get; set; }
    }

    public interface IRows : IRelation<Row>
    {
        void Insert(Row row);
    }

    [Generate]
    public class Root
    {
        public IDictionary<int, Holder>? Holders { get; set; }
        public Holder? Direct { get; set; }
    }

    static void Fill(Holder holder, int i)
    {
        for (var j = 0; j < 5; j++)
        {
            holder.List!.Add((ulong)(i * 10 + j));
            holder.Bitmap!.Set((ulong)(i * 1000 + j * 70000), true);
            holder.Set!.Add(i + j);
            holder.Lazy![j] = i;
        }

        holder.List!.Flush();
        holder.Bitmap!.Flush();
    }

    [Fact]
    public void ValidNestedLazyContentIsNoLeak()
    {
        using (var tr = _db.StartTransaction())
        {
            var rows = tr.GetRelation<IRows>();
            for (var i = 0; i < 3; i++)
            {
                var direct = tr.New<Holder>();
                var inMap = tr.New<Holder>();
                var inList = tr.New<Holder>();
                var nested = tr.New<Holder>();
                inList.Next = nested;
                rows.Insert(new Row { Id = (ulong)i, Direct = direct, Map = new() { [i] = inMap }, Items = [inList] });
            }

            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var i = 0;
            foreach (var row in tr.GetRelation<IRows>())
            {
                Fill(row.Direct!, i++);
                foreach (var holder in row.Map!.Values) Fill(holder, i++);
                foreach (var holder in row.Items!)
                {
                    Fill(holder, i++);
                    Fill(holder.Next!, i++);
                }
            }

            var root = tr.Singleton<Root>();
            root.Direct = tr.New<Holder>();
            Fill(root.Direct, i++);
            root.Holders![1] = tr.New<Holder>();
            Fill(root.Holders[1], i);
            tr.Commit();
        }

        using (var tr = _db.StartReadOnlyTransaction())
        {
            var root = tr.Singleton<Root>();
            Assert.Equal(5ul, root.Direct!.List!.Count);
            Assert.Equal(5ul, root.Holders![1].Bitmap!.Count);
            foreach (var row in tr.GetRelation<IRows>())
            {
                Assert.Equal(5, row.Items![0].Next!.Set!.Count);
                Assert.Equal(5, row.Map![(int)row.Id].Lazy!.Count);
            }
        }

        // Leak removal deletes what detection reports, so content reachable only through nested inline objects must
        // be marked as used.
        var result = _db.RunLeakDetection();
        Assert.Equal(0ul, result.LeakedKeyCount);
    }
}
