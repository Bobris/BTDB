using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using BTDB.BTreeLib;
using BTDB.KVDBLayer;

namespace SimpleTester;

public class NativeVsManagedBugFinder
{
    IKeyValueDB _kv1;
    IKeyValueDB _kv2;

    public NativeVsManagedBugFinder()
    {
        _kv1 = new KeyValueDB(new KeyValueDBOptions().FileCollection =
            new InMemoryFileCollection());
        _kv2 = new BTreeKeyValueDB(new KeyValueDBOptions().FileCollection =
            new InMemoryFileCollection());
    }

    public void Torture()
    {
        var r = new Random(3);
        var opindex = 0;
        var tr1 = _kv1.StartWritingTransaction().GetAwaiter().GetResult();
        var tr2 = _kv2.StartWritingTransaction().GetAwaiter().GetResult();
        /*Task.Run(() =>
        {
            while (true)
            {
                var tr = _kv2.StartReadOnlyTransaction();
                Task.Delay(10).Wait();
                tr.Dispose();
            }
        });*/
        while (true)
        {
            opindex++;
            //VerifyTree(((BTreeKeyValueDBTransaction)tr2).BTreeRoot);
            if (opindex % 50000 == 0)
            {
                var memStats = _kv2.GetNativeMemoryStats();
                Console.WriteLine(opindex + " " + tr1.GetKeyValueCount() + " " + tr2.GetKeyValueCount() + " " +
                                  (memStats.AllocCount - memStats.DeallocCount) + " " +
                                  (memStats.AllocSize - memStats.DeallocSize));
                foreach (var s in tr2.CalcBTreeStats().OrderBy(kv=>kv.Key.Depth).ThenBy(kv=>kv.Key.Children))
                {
                    Console.WriteLine($"Depth {s.Key.Depth} Children {s.Key.Children}: {s.Value}");
                }
                /*
                var res1 = tr1.FindFirstKey(Array.Empty<byte>());
                var res2 = tr2.FindFirstKey(Array.Empty<byte>());
                while (res1)
                {
                    if (res1 != res2) Debugger.Break();
                    var k1 = tr1.GetKeyToArray();
                    var k2 = tr2.GetKeyToArray();
                    if (!k1.SequenceEqual(k2)) Debugger.Break();
                    res1 = tr1.FindNextKey(Array.Empty<byte>());
                    res2 = tr2.FindNextKey(Array.Empty<byte>());
                }
                //*/
            }

            if (opindex % 2 == 0)
            {
                tr1.Commit();
                tr2.Commit();
                tr1.Dispose();
                tr2.Dispose();
                tr1 = _kv1.StartWritingTransaction().GetAwaiter().GetResult();
                tr2 = _kv2.StartWritingTransaction().GetAwaiter().GetResult();
            }

            var op = r.Next(4);
            switch (op)
            {
                case 0: goto case 1;
                case 1:
                {
                    var b = new byte[r.Next(3, 10)];
                    r.NextBytes(b);
                    b[0] = (byte)(b[0] & 1);
                    b[1] = (byte)(b[1] & 1);
                    var res1 = tr1.CreateOrUpdateKeyValue(b, b);
                    var res2 = tr2.CreateOrUpdateKeyValue(b, b);
                    if (res1 != res2)
                    {
                        Console.WriteLine("Create " + opindex + " has different result " + res1 + " " + res2);
                        Debugger.Break();
                        return;
                    }

                    break;
                }
                case 2:
                {
                    var total1 = tr1.GetKeyValueCount();
                    var total2 = tr2.GetKeyValueCount();
                    if (total1 != total2)
                    {
                        Console.WriteLine("Total in remove " + opindex + " has different result " + total1 + " " +
                                          total2);
                        Debugger.Break();
                        return;
                    }

                    if (total1 == 0) break;

                    if (total1 > 10000000000 && r.Next(10000) == 0)
                    {
                        var index1 = r.Next((int)total1);
                        var index2 = r.Next((int)total1);
                        //Console.WriteLine("EraseRange "+index1+" "+index2+" "+total1);
                        tr1.EraseRange(index1, index2);
                        tr2.EraseRange(index1, index2);
                    }
                    else
                    {
                        var index = r.Next((int)total1);
                        var res1 = tr1.SetKeyIndex(index);
                        var res2 = tr2.SetKeyIndex(index);
                        if (res1 != res2)
                        {
                            Console.WriteLine("Remove SetKeyIndex " + opindex + " has different result " + res1 + " " +
                                              res2);
                            Debugger.Break();
                            return;
                        }

                        tr1.EraseCurrent();
                        tr2.EraseCurrent();
                    }

                    break;
                }
                case 3:
                {
                    var b = new byte[r.Next(3, 10)];
                    r.NextBytes(b);
                    b[0] = (byte)(b[0] & 1);
                    b[1] = (byte)(b[1] & 1);
                    var prefix = (uint)r.Next(0, b.Length + 1);
                    var res1 = tr1.Find(b, prefix);
                    var res2 = tr2.Find(b, prefix);
                    if (res1 != res2)
                    {
                        Console.WriteLine("Find " + opindex + " has different result " + res1 + " " + res2);
                        Debugger.Break();
                        return;
                    }

                    break;
                }
            }
        }
    }

    void VerifyTree(IRootNode? node)
    {
        if (node == null) return;
        var cursor = node.CreateCursor();
        cursor.TestTreeCorrectness();
    }
}
