using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using Xunit;

namespace BTDBTest;

/// Random batched and ordinary transactions, some rolled back, with batch finishes, compactions and small log files:
/// after every step the writer, once the batch is published, sees exactly the committed changes, and a reopened
/// database holds them too.
public class TransactionBatchingRandomTest
{
    sealed class SmallLogs : ITransactionLogSizeStrategy
    {
        public TransactionLogSizeLimits GetLimits(uint transactionLogFileId) => new(1024, 1600);
    }

    static BTreeKeyValueDB Open(IFileCollection files, bool split) => new(new KeyValueDBOptions
    {
        FileCollection = files, Compression = new NoCompressionStrategy(), CompactorScheduler = null,
        FileSplitSize = 4096, TransactionLogSizeStrategy = split ? new SmallLogs() : null
    });

    static string Dump(IKeyValueDBTransaction tr)
    {
        using var cursor = tr.CreateCursor();
        var parts = new List<string>();
        if (cursor.FindFirstKey([]))
            do
            {
                var key = Span<byte>.Empty;
                var value = Span<byte>.Empty;
                parts.Add(Convert.ToHexString(cursor.GetKeySpan(ref key)) + "=" +
                          Convert.ToHexString(cursor.GetValueSpan(ref value)));
            } while (cursor.FindNextKey([]));
        return string.Join(";", parts);
    }

    static string Describe(SortedDictionary<string, string> model) =>
        string.Join(";", model.Select(p => p.Key + "=" + p.Value));

    public static TheoryData<int, bool> Seeds()
    {
        var data = new TheoryData<int, bool>();
        for (var seed = 1; seed <= 30; seed++) data.Add(seed, seed % 2 == 0);
        return data;
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task BatchedCommitsAndRollbacksMatchTheModel(int seed, bool split)
    {
        var random = new Random(seed);
        using var files = new InMemoryFileCollection();
        var committed = new SortedDictionary<string, string>(StringComparer.Ordinal);
        // What readers see: a batch is published by finishing it, an ordinary writer, rollback recovery, or compaction
        // when it remaps values in a writing transaction.
        var published = Describe(committed);
        (bool Batch, bool Finish, bool Rollback) step = default;
        using (var db = Open(files, split))
        {
            for (var t = 0; t < 120; t++)
            {
                var batch = random.Next(4) != 0;
                var rollback = random.Next(5) == 0;
                var pending = new SortedDictionary<string, string>(committed, StringComparer.Ordinal);
                using (var tr = await db.StartWritingTransaction(batch))
                {
                    Assert.True(Describe(committed) == Dump(tr), $"Seed {seed} step {t}: the writer sees another state");
                    using var cursor = tr.CreateCursor();
                    var wrote = false;
                    for (var op = random.Next(1, 4); op > 0; op--)
                    {
                        var key = new[] { (byte)random.Next(16) };
                        var hex = Convert.ToHexString(key);
                        if (random.Next(4) == 0)
                        {
                            if (cursor.FindExactKey(key))
                            {
                                cursor.EraseCurrent();
                                wrote = true;
                            }

                            pending.Remove(hex);
                        }
                        else
                        {
                            wrote = true;
                            var value = Enumerable.Repeat((byte)t, random.Next(3) == 0 ? 600 : 3).ToArray();
                            cursor.CreateOrUpdateKeyValue(key, value);
                            pending[hex] = Convert.ToHexString(value);
                        }
                    }

                    var finish = batch && random.Next(5) == 0;
                    if (finish) db.FinishTransactionBatchAfterCurrentTransaction();
                    if (!rollback)
                    {
                        tr.Commit();
                        committed = pending;
                    }

                    // Rolling back a batched transaction that wrote something publishes the replayed batch; one that
                    // wrote nothing leaves the batch pending.
                    if (!batch || finish || (rollback && wrote)) published = Describe(committed);
                    step = (batch, finish, rollback);
                }

                switch (random.Next(10))
                {
                    case 0:
                        await db.Compact(CancellationToken.None);
                        using (var reader = db.StartReadOnlyTransaction())
                            if (Dump(reader) == Describe(committed)) published = Describe(committed);
                        break;
                    case 1:
                        db.CreateKvi(CancellationToken.None);
                        break;
                }

                using (var reader = db.StartReadOnlyTransaction())
                    Assert.True(published == Dump(reader), $"Seed {seed} step {t} {step}: readers see another state");
            }

            using var last = await db.StartWritingTransaction();
            Assert.Equal(Describe(committed), Dump(last));
        }

        using var reopened = Open(files, split);
        using var read = reopened.StartReadOnlyTransaction();
        Assert.Equal(Describe(committed), Dump(read));
    }
}
