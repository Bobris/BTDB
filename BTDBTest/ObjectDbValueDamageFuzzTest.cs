using System;
using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

// Damaged values may fail to read or iterate with any exception, but must neither exhaust memory nor read forever.
public class ObjectDbValueDamageFuzzTest
{
    [Generate]
    public class Inner
    {
        public string? Text { get; set; }
        public List<int>? Numbers { get; set; }
        public Inner? Next { get; set; }
    }

    public class Row
    {
        [PrimaryKey] public ulong Id { get; set; }
        public string? Name { get; set; }
        public List<string>? Tags { get; set; }
        public Dictionary<int, string>? Map { get; set; }
        public Inner? A { get; set; }
        public Inner? B { get; set; }
        public IDictionary<ulong, string>? Lazy { get; set; }
        public byte[]? Blob { get; set; }
    }

    public interface IRows : IRelation<Row>
    {
        void Insert(Row row);
    }

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 300).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void DamagedValuesFailWithoutRunningAway(int seed)
    {
        var random = new Random(seed);
        using var kv = new InMemoryKeyValueDB();
        using (var db = new ObjectDB())
        {
            db.Open(kv, false);
            using var tr = db.StartTransaction();
            var rows = tr.GetRelation<IRows>();
            for (var i = 0; i < 20; i++)
            {
                var shared = new Inner { Text = "s" + i, Numbers = [i, i + 1] };
                rows.Insert(new Row
                {
                    Id = (ulong)i, Name = new string('n', random.Next(20)), Tags = ["a", "b" + i],
                    Map = new() { [i] = "x", [i + 1] = "y" }, A = shared,
                    B = random.Next(2) == 0 ? shared : new Inner { Next = shared },
                    Blob = new byte[random.Next(10)]
                });
            }

            tr.Commit();
        }

        for (var round = 0; round < 30; round++)
        {
            using (var tr = kv.StartWritingTransaction().AsTask().GetAwaiter().GetResult())
            {
                using var cursor = tr.CreateCursor();
                var keys = new List<byte[]>();
                while (cursor.FindNextKey(ObjectDB.AllRelationsPKPrefix)) keys.Add(cursor.SlowGetKey());
                var key = keys[random.Next(keys.Count)];
                Assert.True(cursor.FindExactKey(key));
                var value = cursor.SlowGetValue();
                if (value.Length == 0) continue;
                for (var changes = random.Next(1, 3); changes > 0; changes--)
                    value[random.Next(value.Length)] = (byte)random.Next(256);
                cursor.SetValue(value);
                tr.Commit();
            }

            using var db = new ObjectDB();
            db.Open(kv, false);
            using var read = db.StartReadOnlyTransaction();
            try
            {
                var visitor = new ODBIteratorTest.ToStringVisitor();
                new ODBIterator(read, visitor).Iterate();
                if (visitor.ToString().Length > 50_000_000) throw new InvalidOperationException("Reading does not end");
            }
            catch (Exception error) when (error is not OutOfMemoryException and not InvalidOperationException
                                              { Message: "Reading does not end" })
            {
            }

            try
            {
                var count = 0;
                foreach (var row in read.GetRelation<IRows>())
                {
                    _ = row.Lazy?.Count;
                    _ = row.Tags?.Count;
                    for (var inner = row.A; inner != null; inner = inner.Next)
                        if (++count > 100_000) throw new InvalidOperationException("Reading does not end");
                }
            }
            catch (Exception error) when (error is not OutOfMemoryException and not InvalidOperationException
                                              { Message: "Reading does not end" })
            {
            }
        }
    }

    [Generate]
    public class Root
    {
        public IDictionary<int, Inner>? Items { get; set; }
        public List<Inner>? List { get; set; }
        public Inner? Shared { get; set; }
        public IOrderedSet<string>? Names { get; set; }
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void DamagedObjectsFailWithoutRunningAway(int seed)
    {
        var random = new Random(seed);
        using var kv = new InMemoryKeyValueDB();
        using (var db = new ObjectDB())
        {
            db.Open(kv, false);
            using var tr = db.StartTransaction();
            var root = tr.Singleton<Root>();
            var shared = new Inner { Text = "shared", Numbers = [1, 2, 3] };
            root.Shared = shared;
            root.List = [shared, new Inner { Next = shared }];
            for (var i = 0; i < 20; i++)
            {
                root.Items![i] = new Inner { Text = new string('t', random.Next(10)), Next = i % 3 == 0 ? shared : null };
                root.Names!.Add("n" + i);
            }

            tr.Commit();
        }

        byte[][] prefixes = [ObjectDB.AllObjectsPrefix, ObjectDB.AllDictionariesPrefix];
        for (var round = 0; round < 30; round++)
        {
            using (var tr = kv.StartWritingTransaction().AsTask().GetAwaiter().GetResult())
            {
                using var cursor = tr.CreateCursor();
                var keys = new List<byte[]>();
                foreach (var prefix in prefixes)
                    while (cursor.FindNextKey(prefix)) keys.Add(cursor.SlowGetKey());
                var key = keys[random.Next(keys.Count)];
                Assert.True(cursor.FindExactKey(key));
                var value = cursor.SlowGetValue();
                if (value.Length == 0) continue;
                for (var changes = random.Next(1, 3); changes > 0; changes--)
                    value[random.Next(value.Length)] = (byte)random.Next(256);
                cursor.SetValue(value);
                tr.Commit();
            }

            using var db = new ObjectDB();
            db.Open(kv, false);
            using var read = db.StartReadOnlyTransaction();
            try
            {
                var visitor = new ODBIteratorTest.ToStringVisitor();
                new ODBIterator(read, visitor).Iterate();
                if (visitor.ToString().Length > 50_000_000) throw new InvalidOperationException("Reading does not end");
            }
            catch (Exception error) when (error is not OutOfMemoryException and not InvalidOperationException
                                              { Message: "Reading does not end" })
            {
            }

            try
            {
                var root = read.Singleton<Root>();
                var count = 0;
                foreach (var pair in root.Items!)
                    for (var inner = pair.Value; inner != null; inner = inner.Next)
                        if (++count > 100_000) throw new InvalidOperationException("Reading does not end");
                foreach (var inner in root.List ?? []) _ = inner?.Numbers?.Count;
                foreach (var name in root.Names!) _ = name;
            }
            catch (Exception error) when (error is not OutOfMemoryException and not InvalidOperationException
                                              { Message: "Reading does not end" })
            {
            }
        }
    }
}
