using System.Collections.Generic;
using System.Threading;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

public class TestCompactor
{
    public class Map
    {
        public IDictionary<ulong, ByteBuffer>? Data { get; set; }
    }

    public void Run(CancellationToken cancellation)
    {
        var data = ByteBuffer.NewSync(new byte[512]);
        using var db = CreateDb();
        var counter = 0ul;
        ulong step = 1000;
        while (!cancellation.IsCancellationRequested)
        {
            var tr = db.StartWritingTransaction().Result;
            var m = tr.Singleton<Map>();
            for (var i = 0ul; i < step; i++)
            {
                m.Data![counter + i] = data;
            }

            if (counter >= step)
            {
                for (var i = 0ul; i < step; i++)
                    m.Data!.Remove(counter - step + i);
            }

            counter += 1000;
            tr.Commit();
        }
    }

    static ObjectDB CreateDb()
    {
        var fc = new OnDiskFileCollection(".");
        var lowDb = new BTreeKeyValueDB(fc, new NoCompressionStrategy());
        var db = new ObjectDB();
        db.Open(lowDb, true);
        return db;
    }
}
