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
                Console.Write(Convert.ToHexString(unseenKey.Key));
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

            byte[]?[] lastUnseenKeysD = new byte[10][];
            byte[]?[] lastUnseenKeysO = new byte[10][];

            int indexD = 0, indexO = 0;
            foreach (var unseenKey in visitor.UnseenKeys())
            {
                var isDict = unseenKey.Key[0] == 2;
                var isObject = unseenKey.Key[0] == 1;

                var r = new SpanReader(unseenKey.Key);
                r.SkipUInt8();
                var oid = r.ReadVUInt64();

                if (isDict)
                {
                    leakedDictionaries.TryAdd(oid, false);
                    lastUnseenKeysD[indexD++ % 10] = unseenKey.Key;
                }
                else if (isObject)
                {
                    leakedObjects.TryAdd(oid, false);
                    lastUnseenKeysO[indexO++ % 10] = unseenKey.Key;
                }
            }

            WriteValidatingSamples("testDicts", lastUnseenKeysO, tr);
            WriteValidatingSamples("testObjs", lastUnseenKeysD, tr);
            WriteSplitIdList(leakedDictionaries.Keys, "dicts", 1000);
            WriteSplitIdList(leakedObjects.Keys, "objs", 1000);
        }

        // write last few leaked keys to validate that are present in the exact form on all db replicas before cleaning
        static void WriteValidatingSamples(string varName, byte[]?[] lastUnseenKeys, IObjectDBTransaction tr)
        {
            var sb = new StringBuilder();
            sb.Append($"var {varName} = new Dictionary<string, string>{{\n");
            foreach (var key in lastUnseenKeys)
            {
                if (key == null) continue;
                sb.Append("[\"");
                sb.Append(Convert.ToHexString(key));
                sb.Append("\"] = \"");
                if (tr.KeyValueDBTransaction.FindFirstKey(key))
                {
                    var value = tr.KeyValueDBTransaction.GetValue();
                    if (value.Length > 20)
                        value = value.Slice(0, 20);
                    sb.Append(Convert.ToHexString(value));
                }
                sb.Append('"');
                sb.Append(", \n");
            }

            sb.Append("};");
            Console.WriteLine(sb.ToString());
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
