using System;
using System.Collections.Generic;
using System.Text;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using ODbDump.Visitor;

namespace ODbDump
{
    public static class DataLeakExtension
    {
        public static void DumpLeaks(this IObjectDB db)
        {
            using var tr = db.StartReadOnlyTransaction();
            using var visitor = new FindUnusedKeysVisitor();
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
                        var r = new SpanReader(unseenKey.Key);
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

        public static void DumpLeaksCode(this IObjectDB db)
        {
            var leakedObjects = new Dictionary<ulong, bool>();
            var leakedDictionaries = new Dictionary<ulong, bool>();

            using var tr = db.StartReadOnlyTransaction();
            using var visitor = new FindUnusedKeysVisitor();
            visitor.ImportAllKeys(tr);
            visitor.Iterate(tr);
            foreach (var unseenKey in visitor.UnseenKeys())
            {
                var isDict = unseenKey.Key[0] == 2;
                var isObject = unseenKey.Key[0] == 1;

                var r = new SpanReader(unseenKey.Key);
                r.SkipUInt8();
                var oid = r.ReadVUInt64();

                if (isDict)
                    leakedDictionaries.TryAdd(oid, false);
                else if (isObject)
                    leakedObjects.TryAdd(oid, false);
            }

            WriteSplitIdList(leakedDictionaries.Keys, "dicts", 1000);
            WriteSplitIdList(leakedObjects.Keys, "objs", 1000);
        }

        static void WriteSplitIdList(IEnumerable<ulong> objIds, string name, int count)
        {
            var sb = InitStringBuilder(name);
            int subIdx = 0;
            foreach (var id in objIds)
            {
                if (subIdx++ > 0)
                    sb.Append(",");
                sb.Append(id);

                if (subIdx >= count)
                {
                    sb.Append("};");
                    Console.WriteLine(sb);
                    subIdx = 0;
                    sb = InitStringBuilder(name);
                }
            }

            if (subIdx > 0)
            {
                sb.Append("};");
                Console.WriteLine(sb);
            }
        }

        static StringBuilder InitStringBuilder(string name)
        {
            var sb = new StringBuilder();
            sb.Append($"var {name} = new ulong[] {{");
            return sb;
        }
    }
}
