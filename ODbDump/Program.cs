using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace ODbDump
{
    class ToConsoleFastVisitor : IODBFastVisitor
    {
        private StringBuilder _builder = new StringBuilder();
        public void MarkCurrentKeyAsUsed(IKeyValueDBTransaction tr)
        {
        }

        void Print(ByteBuffer b)
        {
            _builder.Clear();
            for (int i = 0; i < b.Length; i++)
            {
                if (i > 0) _builder.Append(' ');
                _builder.Append(b[i].ToString("X2"));
            }
            Console.Write(_builder.ToString());
        }
    }

    class ToConsoleVisitor : ToConsoleFastVisitor, IODBVisitor
    {
        public bool VisitSingleton(uint tableId, string tableName, ulong oid)
        {
            Console.WriteLine("Singleton {0}-{1} oid:{2}", tableId, tableName ?? "?Unknown?", oid);
            return true;
        }

        public bool StartObject(ulong oid, uint tableId, string tableName, uint version)
        {
            Console.WriteLine("Object oid:{0} {1}-{2} version:{3}", oid, tableId, tableName ?? "?Unknown?",
                version);
            return true;
        }

        public bool StartField(string name)
        {
            Console.WriteLine($"StartField {name}");
            return true;
        }

        public bool NeedScalarAsObject()
        {
            return true;
        }

        public void ScalarAsObject(object content)
        {
            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "ScalarObj {0}", content));
        }

        public bool NeedScalarAsText()
        {
            return true;
        }

        public void ScalarAsText(string content)
        {
            Console.WriteLine($"ScalarStr {content}");
        }

        public void OidReference(ulong oid)
        {
            Console.WriteLine($"OidReference {oid}");
        }

        public bool StartInlineObject(uint tableId, string tableName, uint version)
        {
            Console.WriteLine($"StartInlineObject {tableId}-{tableName}-{version}");
            return true;
        }

        public void EndInlineObject()
        {
            Console.WriteLine("EndInlineObject");
        }

        public bool StartList()
        {
            Console.WriteLine("StartList");
            return true;
        }

        public bool StartItem()
        {
            Console.WriteLine("StartItem");
            return true;
        }

        public void EndItem()
        {
            Console.WriteLine("EndItem");
        }

        public void EndList()
        {
            Console.WriteLine("EndList");
        }

        public bool StartDictionary()
        {
            Console.WriteLine("StartDictionary");
            return true;
        }

        public bool StartDictKey()
        {
            Console.WriteLine("StartDictKey");
            return true;
        }

        public void EndDictKey()
        {
            Console.WriteLine("EndDictKey");
        }

        public bool StartDictValue()
        {
            Console.WriteLine("StartDictValue");
            return true;
        }

        public void EndDictValue()
        {
            Console.WriteLine("EndDictValue");
        }

        public void EndDictionary()
        {
            Console.WriteLine("EndDictionary");
        }

        public void EndField()
        {
            Console.WriteLine("EndField");
        }

        public void EndObject()
        {
            Console.WriteLine("EndObject");
        }

        public bool VisitRelation(string relationName)
        {
            Console.WriteLine($"Relation {relationName}");
            return true;
        }

        public bool StartRelationKey()
        {
            Console.WriteLine("BeginKey");
            return true;
        }

        public void EndRelationKey()
        {
            Console.WriteLine("EndKey");
        }

        public bool StartRelationValue()
        {
            Console.WriteLine("BeginValue");
            return true;
        }

        public void EndRelationValue()
        {
            Console.WriteLine("EndValue");
        }

        public void InlineBackRef(int iid)
        {
            Console.WriteLine($"Inline back ref {iid}");
        }

        public void InlineRef(int iid)
        {
            Console.WriteLine($"Inline ref {iid}");
        }
    }

    class ToNullVisitor : ToConsoleFastVisitor, IODBVisitor
    {
        public bool VisitSingleton(uint tableId, string tableName, ulong oid)
        {
            return true;
        }

        public bool StartObject(ulong oid, uint tableId, string tableName, uint version)
        {
            return true;
        }

        public bool StartField(string name)
        {
            return true;
        }

        public bool NeedScalarAsObject()
        {
            return true;
        }

        public void ScalarAsObject(object content)
        {
        }

        public bool NeedScalarAsText()
        {
            return true;
        }

        public void ScalarAsText(string content)
        {
        }

        public void OidReference(ulong oid)
        {
        }

        public bool StartInlineObject(uint tableId, string tableName, uint version)
        {
            return true;
        }

        public void EndInlineObject()
        {
        }

        public bool StartList()
        {
            return true;
        }

        public bool StartItem()
        {
            return true;
        }

        public void EndItem()
        {
        }

        public void EndList()
        {
        }

        public bool StartDictionary()
        {
            return true;
        }

        public bool StartDictKey()
        {
            return true;
        }

        public void EndDictKey()
        {
        }

        public bool StartDictValue()
        {
            return true;
        }

        public void EndDictValue()
        {
        }

        public void EndDictionary()
        {
        }

        public void EndField()
        {
        }

        public void EndObject()
        {
        }

        public bool VisitRelation(string relationName)
        {
            return true;
        }

        public bool StartRelationKey()
        {
            return true;
        }

        public void EndRelationKey()
        {
        }

        public bool StartRelationValue()
        {
            return true;
        }

        public void EndRelationValue()
        {
        }

        public void InlineBackRef(int iid)
        {
        }

        public void InlineRef(int iid)
        {
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Need to have just one parameter with directory of ObjectDB");
                return;
            }
            var action = "dump";
            if (args.Length > 1)
            {
                action = args[1].ToLowerInvariant();
            }

            switch (action)
            {
                case "dump":
                    {
                        using (var dfc = new OnDiskFileCollection(args[0]))
                        using (var kdb = new KeyValueDB(dfc))
                        using (var odb = new ObjectDB())
                        {
                            odb.Open(kdb, false);
                            using (var trkv = kdb.StartReadOnlyTransaction())
                            using (var tr = odb.StartTransaction())
                            {
                                Console.WriteLine("CommitUlong: " + tr.GetCommitUlong());
                                Console.WriteLine("Ulong[0] oid: " + trkv.GetUlong(0));
                                Console.WriteLine("Ulong[1] dictid: " + trkv.GetUlong(1));
                                var visitor = new ToConsoleVisitor();
                                var iterator = new ODBIterator(tr, visitor);
                                iterator.Iterate();
                            }
                        }
                        break;
                    }
                case "dumpnull":
                case "null":
                    {
                        using (var dfc = new OnDiskFileCollection(args[0]))
                        using (var kdb = new KeyValueDB(dfc))
                        using (var odb = new ObjectDB())
                        {
                            odb.Open(kdb, false);
                            using (var tr = odb.StartTransaction())
                            {
                                var visitor = new ToNullVisitor();
                                var iterator = new ODBIterator(tr, visitor);
                                iterator.Iterate();
                            }
                        }
                        break;
                    }
                case "stat":
                    {
                        using (var dfc = new OnDiskFileCollection(args[0]))
                        using (var kdb = new KeyValueDB(dfc))
                        {
                            Console.WriteLine(kdb.CalcStats());
                        }
                        break;
                    }
                case "compact":
                    {
                        using (var dfc = new OnDiskFileCollection(args[0]))
                        using (var kdb = new KeyValueDB(dfc, new SnappyCompressionStrategy(), 100 * 1024 * 1024, null))
                        {
                            Console.WriteLine("Starting first compaction");
                            while (kdb.Compact(new CancellationToken()))
                            {
                                Console.WriteLine(kdb.CalcStats());
                                Console.WriteLine("Another compaction needed");
                            }
                            Console.WriteLine(kdb.CalcStats());
                        }
                        break;
                    }
                case "export":
                    {
                        using (var dfc = new OnDiskFileCollection(args[0]))
                        using (var kdb = new KeyValueDB(dfc))
                        using (var tr = kdb.StartReadOnlyTransaction())
                        using (var st = File.Create(Path.Combine(args[0], "export.dat")))
                        {
                            KeyValueDBExportImporter.Export(tr, st);
                        }
                        break;
                    }
                default:
                    {
                        Console.WriteLine($"Unknown action: {action}");
                        break;
                    }
            }
        }
    }
}
