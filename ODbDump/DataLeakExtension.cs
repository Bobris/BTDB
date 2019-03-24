using System;
using System.Collections.Generic;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using ODbDump.Visitor;

namespace ODbDump
{
    public static class DataLeakExtension
    {
        public static void DumpLeaks(this IObjectDB db)
        {
            using (var tr = db.StartReadOnlyTransaction())
            using (var visitor = new FindUnusedKeysVisitor())
            {
                visitor.ImportAllKeys(tr);
                var iterator = visitor.Iterate(tr);
                visitor.DumpUnseenKeys();
                var leakedObjects = new List<ulong>();
                foreach (var unseenKey in visitor.UnseenKeys())
                {
                    if (unseenKey.Key[0] == 1)
                    {
                        try
                        {
                            var r = new ByteArrayReader(unseenKey.Key);
                            r.SkipUInt8();
                            var oid = r.ReadVUInt64();
                            leakedObjects.Add(oid);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Leak found but error has occured while reading: {ex.Message}");
                        }
                    }
                }

                if (leakedObjects.Count > 0)
                {
                    Console.WriteLine("--- OBJECTS ---");
                    var consoleVisitor = new ToConsoleVisitorNice();
                    foreach (var oid in leakedObjects)
                    {
                        iterator.IterateUnseenOid(oid, consoleVisitor);
                        Console.WriteLine("------");
                    }
                }
            }
        }

        static void DumpUnseenKeys(this FindUnusedKeysVisitor visitor)
        {
            foreach (var unseenKey in visitor.UnseenKeys())
            {
                foreach (var b in unseenKey.Key)
                {
                    Console.Write(' ');
                    Console.Write(b.ToString("X2"));
                }

                Console.Write(" Value len:");
                Console.WriteLine(unseenKey.ValueSize);
            }
        }
    }
}