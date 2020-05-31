using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace SimpleTester
{
    public class CompactionStressTest
    {
        public void Run(string dbDirectory, CancellationToken cancellationToken)
        {
            using var fc = new OnDiskFileCollection(dbDirectory);
            using var lowDb = new KeyValueDB(fc, new NoCompressionStrategy(), 2000);

            var values = new List<byte[]>(100);
            for (var i = 0; i < 100; i++)
            {
                values.Add(new byte[i]);
            }

            var rnd = new Random();

            var mre = new ManualResetEvent(false);

            var t = Task.Run(async () =>
            {
                var keyBuffer = new byte[10];
                while (true)
                {
                    using var tr = await lowDb.StartWritingTransaction();
                    for (var j = 0; j < 10000; j++)
                    {
                        var pos = 0;
                        PackUnpack.PackVInt(keyBuffer, ref pos, j);
                        tr.CreateOrUpdateKeyValue(keyBuffer.AsSpan(0, pos), values[rnd.Next(0, 99)]);

                        if (cancellationToken.IsCancellationRequested)
                            return;
                    }

                    Console.WriteLine("w ");
                    tr.Commit();
                    mre.Set();
                }
            }, cancellationToken);

            if (!cancellationToken.IsCancellationRequested)
                mre.WaitOne();

            var r1 = StartReader(lowDb, "r1", 1, cancellationToken);
            var r2 = StartReader(lowDb, "r2", 10, cancellationToken);
            var r3 = StartReader(lowDb, "r3", 100, cancellationToken);


            Task.WaitAll(t, r1, r2, r3);
        }

        static Task StartReader(IKeyValueDB lowDb, string id, int sleep, CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                while (true)
                {
                    var transaction = lowDb.StartReadOnlyTransaction();
                    transaction.FindFirstKey();
                    var keyValueCount = transaction.GetKeyValueCount();
                    for (long kv = 0; kv < keyValueCount; kv++)
                    {
                        transaction.GetKey();
                        transaction.GetValue();
                        transaction.FindNextKey();
                        await Task.Delay(sleep, cancellationToken);

                        if (cancellationToken.IsCancellationRequested)
                            return;
                    }

                    Console.Write($"{id} ");
                }
            }, cancellationToken);
        }
    }
}