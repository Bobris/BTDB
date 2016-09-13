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
    class ToStringFastVisitor : IODBFastVisitor
    {
        protected readonly StringBuilder Builder = new StringBuilder();

        public override string ToString()
        {
            return Builder.ToString();
        }

        public void MarkCurrentKeyAsUsed(IKeyValueDBTransaction tr)
        {
        }

        void Print(ByteBuffer b)
        {
            for (int i = 0; i < b.Length; i++)
            {
                if (i > 0) Builder.Append(' ');
                Builder.Append(b[i].ToString("X2"));
            }
        }
    }

    class ToStringVisitor : ToStringFastVisitor, IODBVisitor
    {
        public bool VisitSingleton(uint tableId, string tableName, ulong oid)
        {
            Builder.AppendFormat("Singleton {0}-{1} oid:{2}", tableId, tableName ?? "?Unknown?", oid);
            Builder.AppendLine();
            return true;
        }

        public bool StartObject(ulong oid, uint tableId, string tableName, uint version)
        {
            Builder.AppendFormat("Object oid:{0} {1}-{2} version:{3}", oid, tableId, tableName ?? "?Unknown?",
                version);
            Builder.AppendLine();
            return true;
        }

        public bool StartField(string name)
        {
            Builder.AppendLine($"StartField {name}");
            return true;
        }

        public bool NeedScalarAsObject()
        {
            return true;
        }

        public void ScalarAsObject(object content)
        {
            Builder.AppendLine(string.Format(CultureInfo.InvariantCulture, "ScalarObj {0}", content));
        }

        public bool NeedScalarAsText()
        {
            return true;
        }

        public void ScalarAsText(string content)
        {
            Builder.AppendLine($"ScalarStr {content}");
        }

        public void OidReference(ulong oid)
        {
            Builder.AppendLine($"OidReference {oid}");
        }

        public bool StartInlineObject(uint tableId, string tableName, uint version)
        {
            Builder.AppendLine($"StartInlineObject {tableId}-{tableName}-{version}");
            return true;
        }

        public void EndInlineObject()
        {
            Builder.AppendLine("EndInlineObject");
        }

        public bool StartList()
        {
            Builder.AppendLine("StartList");
            return true;
        }

        public bool StartItem()
        {
            Builder.AppendLine("StartItem");
            return true;
        }

        public void EndItem()
        {
            Builder.AppendLine("EndItem");
        }

        public void EndList()
        {
            Builder.AppendLine("EndList");
        }

        public bool StartDictionary()
        {
            Builder.AppendLine("StartDictionary");
            return true;
        }

        public bool StartDictKey()
        {
            Builder.AppendLine("StartDictKey");
            return true;
        }

        public void EndDictKey()
        {
            Builder.AppendLine("EndDictKey");
        }

        public bool StartDictValue()
        {
            Builder.AppendLine("StartDictValue");
            return true;
        }

        public void EndDictValue()
        {
            Builder.AppendLine("EndDictValue");
        }

        public void EndDictionary()
        {
            Builder.AppendLine("EndDictionary");
        }

        public void EndField()
        {
            Builder.AppendLine("EndField");
        }

        public void EndObject()
        {
            Builder.AppendLine("EndObject");
        }

        public bool VisitRelation(string relationName)
        {
            Builder.AppendFormat($"Relation {relationName}");
            Builder.AppendLine();
            return true;
        }

        public bool StartRelationKey()
        {
            Builder.AppendLine("BeginKey");
            return true;
        }

        public void EndRelationKey()
        {
            Builder.AppendLine("EndKey");
        }

        public bool StartRelationValue()
        {
            Builder.AppendLine("BeginValue");
            return true;
        }

        public void EndRelationValue()
        {
            Builder.AppendLine("EndValue");
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
                            using (var tr = odb.StartTransaction())
                            {
                                var visitor = new ToStringVisitor();
                                var iterator = new ODBIterator(tr, visitor);
                                iterator.Iterate();
                                var text = visitor.ToString();
                                Console.WriteLine(text);
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
