using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using static BTDBTest.RelationMutationDifferentialTest;

namespace BTDBTest;

/// ObjectDB over a key value database that crashes at any file operation of relation changes, compactions and key
/// index writes opens, holds the rows of one of its commits with every secondary index matching them, and takes new
/// rows that survive the next open.
public class ObjectDbCrashSimulationTest
{
    static KeyValueDBOptions Options(IFileCollection files) => new()
    {
        FileCollection = files, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
        FileSplitSize = 4096
    };

    static readonly string?[] Names = [null, "", "a", "b"];

    static string Describe(IRowTable table) =>
        string.Join(";", table.Select(r => $"{r.Tenant}/{r.Id} {r.Age} {r.Name ?? "null"} {r.Payload}"));

    static async Task<List<string>> Workload(KeyValueDBCrashSimulationTest.RecordingCollection files, int seed)
    {
        var random = new Random(seed);
        var states = new List<string> { "" };
        using var kv = new BTreeKeyValueDB(Options(files));
        // Durable commits add a hard flush, so a crash point, to every commit.
        kv.DurableTransactions = seed % 2 == 0;
        using var db = new ObjectDB();
        db.Open(kv, false);
        for (var t = 0; t < 25; t++)
        {
            using (var tr = db.StartTransaction())
            {
                var table = tr.GetRelation<IRowTable>();
                for (var op = random.Next(1, 4); op > 0; op--)
                {
                    var tenant = (ulong)random.Next(1, 3);
                    var id = (ulong)random.Next(1, 8);
                    if (random.Next(4) == 0) table.RemoveById(tenant, id);
                    else
                        table.Upsert(new()
                        {
                            Tenant = tenant, Id = id, Age = (uint)random.Next(20, 23), Name = Names[random.Next(Names.Length)],
                            Payload = random.Next(3) == 0 ? random.Next() : t
                        });
                }

                tr.Commit();
            }

            using (var tr = db.StartReadOnlyTransaction()) states.Add(Describe(tr.GetRelation<IRowTable>()));
            if (t % 6 == 5) await kv.Compact(CancellationToken.None);
            if (t % 8 == 7) kv.CreateKvi(CancellationToken.None);
        }

        return states;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task EveryCrashOpensToACommittedRelationWithMatchingIndexes(int seed)
    {
        int total;
        List<string> states;
        using (var counting = new KeyValueDBCrashSimulationTest.RecordingCollection())
        {
            states = await Workload(counting, seed);
            total = counting.Operations;
        }

        Assert.True(total > 10, $"Only {total} file operations to crash at");
        var random = new Random(seed);
        for (var crash = 0; crash < total; crash++)
        {
            using var files = new KeyValueDBCrashSimulationTest.RecordingCollection { CrashAt = crash };
            await Workload(files, seed);
            var directory = Path.Combine(Path.GetTempPath(), "btdb-odb-crash-" + Guid.NewGuid().ToString("N"));
            try
            {
                KeyValueDBCrashSimulationTest.WriteSnapshot(files, directory, random);
                string opened;
                using (var kv = new BTreeKeyValueDB(Options(new OnDiskFileCollection(directory))))
                using (var db = new ObjectDB())
                {
                    db.Open(kv, false);
                    using (var tr = db.StartTransaction())
                    {
                        var table = tr.GetRelation<IRowTable>();
                        opened = Describe(table);
                        var rows = table.ToList();
                        foreach (var name in Names)
                            Assert.Equal(rows.Where(r => r.Name == name).Select(r => (r.Tenant, r.Id)).Order(),
                                table.FindByName(name).Select(r => (r.Tenant, r.Id)).Order());
                        foreach (var tenant in new ulong[] { 1, 2 })
                        for (var age = 20u; age < 23; age++)
                            Assert.Equal(rows.Where(r => r.Tenant == tenant && r.Age == age).Select(r => r.Id),
                                table.FindByAge(tenant, age).Select(r => r.Id));
                        table.Upsert(new() { Tenant = 9, Id = 9, Age = 99, Name = "new", Payload = 1 });
                        tr.Commit();
                    }
                }

                Assert.True(states.Contains(opened), $"Seed {seed} crash {crash}/{total}: opened rows no commit made");
                using var again = new BTreeKeyValueDB(Options(new OnDiskFileCollection(directory)));
                using var reopened = new ObjectDB();
                reopened.Open(again, false);
                using var read = reopened.StartReadOnlyTransaction();
                var expected = opened.Length == 0 ? "9/9 99 new 1" : opened + ";9/9 99 new 1";
                Assert.Equal(expected, Describe(read.GetRelation<IRowTable>()));
            }
            finally
            {
                if (Directory.Exists(directory)) Directory.Delete(directory, true);
            }
        }
    }
}
