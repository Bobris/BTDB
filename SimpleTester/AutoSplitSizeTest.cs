using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDBTest;

namespace SimpleTester;

public class AutoSplitSizeTest
{
    public void Run(string dbDirectory, CancellationToken cancellationToken)
    {
        Directory.Delete(dbDirectory, true);
        Directory.CreateDirectory(dbDirectory);
        using var fc = new OnDiskFileCollection(dbDirectory);
        using var lowDb = new BTreeKeyValueDB(fc, new NoCompressionStrategy());
        lowDb.AutoAdjustFileSize = true;

        var j = 0;
        var keyBuffer = new byte[10];
        var value = new byte[50000];
        // write a lot of data
        while (!cancellationToken.IsCancellationRequested)
        {
            using var tr = lowDb.StartTransaction();
            using var cursor = tr.CreateCursor();
            var pos = 0;
            PackUnpack.PackVInt(keyBuffer, ref pos, j);
            cursor.CreateOrUpdateKeyValue(keyBuffer.AsSpan(0, pos), value);
            pos = 0;
            PackUnpack.PackVInt(keyBuffer, ref pos, j / 2);
            if (cursor.FindExactKey(keyBuffer.AsSpan(0, pos)))
                cursor.EraseCurrent();
            tr.Commit();
            j++;
            if (j % 1000 == 0)
            {
                var totalSize = 0UL;
                var count = 0;
                foreach (var collectionFile in fc.Enumerate())
                {
                    totalSize += collectionFile.GetSize();
                    count++;
                    Console.Write($"{(collectionFile.GetSize() + 1024 * 1024 - 1) / 1024 / 1024} ");
                }

                Console.WriteLine();
                Console.WriteLine("Total size: " + (totalSize + 1024 * 1024 - 1) / 1024 / 1024 + "MB Count: " + count);
            }

            if (j % 10000 == 0)
            {
                Console.WriteLine("Compact");
                lowDb.Compact(new CancellationToken());
            }
        }
    }
}
