using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using System.Collections.Generic;
using System.Diagnostics;

namespace ODbDump
{
    class ToConsoleFastVisitor : IODBFastVisitor
    {
        internal int _indent = 0;

        readonly StringBuilder _builder = new StringBuilder();

        public void MarkCurrentKeyAsUsed(IKeyValueDBTransaction tr)
        {
        }

        public virtual void Print(string s)
        {
            Console.WriteLine(new string(' ', _indent * 2) + s);
        }

        void Print(ByteBuffer b)
        {
            _builder.Clear();
            for (int i = 0; i < b.Length; i++)
            {
                if (i > 0) _builder.Append(' ');
                _builder.Append(b[i].ToString("X2"));
            }

            Print(_builder.ToString());
        }
    }

    class ToFileVisitorNice : ToConsoleVisitorNice
    {
        readonly StreamWriter _streamWriter;

        public ToFileVisitorNice(StreamWriter streamWriter)
        {
            _streamWriter = streamWriter;
        }

        public override void Print(string s)
        {
            _streamWriter.WriteLine(new string(' ', _indent * 2) + s);
        }
    }

    class ToConsoleVisitorNice : ToConsoleFastVisitor, IODBVisitor
    {
        string _currentFieldName;
        readonly Stack<int> _listItemIndexStack = new Stack<int>();
        int _itemIndex;
        int _iid;

        public bool VisitSingleton(uint tableId, string tableName, ulong oid)
        {
            Print($"Singleton {tableId}-{tableName ?? "?Unknown?"} oid:{oid}");
            return true;
        }

        public bool StartObject(ulong oid, uint tableId, string tableName, uint version)
        {
            _indent++;
            Print($"Object oid:{oid} {tableId}-{tableName ?? "?Unknown?"} version:{version}");
            return true;
        }

        public bool StartField(string name)
        {
            _currentFieldName = name;
            return true;
        }

        public bool NeedScalarAsObject()
        {
            return false;
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
            Print($"{_currentFieldName}: {content}");
        }

        public void OidReference(ulong oid)
        {
            Print($"{_currentFieldName}: Oid#{oid}");
        }

        public bool StartInlineObject(uint tableId, string tableName, uint version)
        {
            Print($"{_currentFieldName}: InlineObject {tableId}-{tableName}-{version} ref#{_iid}");
            _indent++;
            return true;
        }

        public void EndInlineObject()
        {
            _indent--;
        }

        public bool StartList()
        {
            Print($"{_currentFieldName}: Array");
            _listItemIndexStack.Push(_itemIndex);
            _itemIndex = 0;
            _indent++;
            return true;
        }

        public bool StartItem()
        {
            _currentFieldName = $"[{_itemIndex}]";
            return true;
        }

        public void EndItem()
        {
            _itemIndex++;
        }

        public void EndList()
        {
            _itemIndex = _listItemIndexStack.Pop();
            _indent--;
        }

        public bool StartDictionary()
        {
            Print($"{_currentFieldName}: Dictionary");
            _listItemIndexStack.Push(_itemIndex);
            _itemIndex = 0;
            _indent++;
            return true;
        }

        public bool StartDictKey()
        {
            _currentFieldName = $"Key[{_itemIndex}]";
            return true;
        }

        public void EndDictKey()
        {
        }

        public bool StartDictValue()
        {
            _currentFieldName = $"Value[{_itemIndex}]";
            return true;
        }

        public void EndDictValue()
        {
            _itemIndex++;
        }

        public void EndDictionary()
        {
            _itemIndex = _listItemIndexStack.Pop();
            _indent--;
        }

        public void EndField()
        {
        }

        public void EndObject()
        {
            _indent--;
        }

        public bool StartRelation(string relationName)
        {
            Print($"Relation {relationName}");
            _listItemIndexStack.Push(_itemIndex);
            _itemIndex = 0;
            _indent++;
            return true;
        }

        public bool StartRelationKey()
        {
            Print($"Key[{_itemIndex}]");
            _indent++;
            return true;
        }

        public void EndRelationKey()
        {
            _indent--;
        }

        public bool StartRelationValue()
        {
            Print($"Value[{_itemIndex}]");
            _indent++;
            return true;
        }

        public void EndRelationValue()
        {
            _itemIndex++;
            _indent--;
        }

        public void EndRelation()
        {
            _itemIndex = _listItemIndexStack.Pop();
            _indent--;
        }

        public void InlineBackRef(int iid)
        {
            Print($"{_currentFieldName}: Inline back ref#{iid}");
        }

        public void InlineRef(int iid)
        {
            _iid = iid;
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

        public bool StartRelation(string relationName)
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

        public void EndRelation()
        {
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

        public bool StartRelation(string relationName)
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

        public void EndRelation()
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

            var action = "nicedump";
            if (args.Length > 1)
            {
                action = args[1].ToLowerInvariant();
            }

            switch (action)
            {
                case "nicedump":
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
                            var visitor = new ToConsoleVisitorNice();
                            var iterator = new ODBIterator(tr, visitor);
                            iterator.Iterate();
                        }
                    }

                    break;
                }
                case "diskdump":
                {
                    using (var dfc = new OnDiskFileCollection(args[0]))
                    using (var kdb = new KeyValueDB(dfc))
                    using (var odb = new ObjectDB())
                    using (var tst = File.CreateText(Path.Combine(args[0], "dump.txt")))
                    {
                        odb.Open(kdb, false);
                        using (var trkv = kdb.StartReadOnlyTransaction())
                        using (var tr = odb.StartTransaction())
                        {
                            tst.WriteLine("CommitUlong: " + tr.GetCommitUlong());
                            tst.WriteLine("Ulong[0] oid: " + trkv.GetUlong(0));
                            tst.WriteLine("Ulong[1] dictid: " + trkv.GetUlong(1));
                            var visitor = new ToFileVisitorNice(tst);
                            var iterator = new ODBIterator(tr, visitor);
                            iterator.Iterate();
                        }
                    }

                    break;
                }
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
                case "fileheaders":
                {
                    using (var dfc = new OnDiskFileCollection(args[0]))
                    {
                        var fcfi = new FileCollectionWithFileInfos(dfc);
                        foreach (var fi in fcfi.FileInfos)
                        {
                            var details = "";
                            switch (fi.Value)
                            {
                                case IKeyIndex keyindex:
                                {
                                    details =
                                        $"KVCount:{keyindex.KeyValueCount} CommitUlong:{keyindex.CommitUlong} TrLogFileId:{keyindex.TrLogFileId} TrLogOffset:{keyindex.TrLogOffset}";
                                    var usedFiles = keyindex.UsedFilesInOlderGenerations;
                                    if (usedFiles != null)
                                    {
                                        details += " UsedFiles:" + string.Join(",", usedFiles);
                                    }

                                    break;
                                }
                                case IFileTransactionLog trlog:
                                    details = string.Format("Previous File Id: {0}", trlog.PreviousFileId);
                                    break;
                            }

                            Console.WriteLine("File {0} Guid:{3} Gen:{2} Type:{1} {4}", fi.Key,
                                fi.Value.FileType.ToString(), fi.Value.Generation, fi.Value.Guid, details);
                        }
                    }

                    break;
                }
                case "compact":
                {
                    var sw = new Stopwatch();
                    sw.Start();
                    using (var dfc = new OnDiskFileCollection(args[0]))
                    using (var kdb = new ArtKeyValueDB(dfc, new SnappyCompressionStrategy(), 100 * 1024 * 1024, null))
                    {
                        kdb.Logger = new ConsoleKvdbLogger();
                        sw.Stop();
                        Console.WriteLine($"Opened in {sw.Elapsed.TotalSeconds:F1}");
                        sw.Restart();
                        while (kdb.Compact(new CancellationToken()))
                        {
                            sw.Stop();
                            Console.WriteLine($"Compaction iteration in {sw.Elapsed.TotalSeconds:F1}");
                            sw.Restart();
                        }

                        sw.Stop();
                        Console.WriteLine($"Final compaction in {sw.Elapsed.TotalSeconds:F1}");
                        Console.WriteLine(kdb.CalcStats());
                    }

                    break;
                }
                case "export":
                {
                    using (var dfc = new OnDiskFileCollection(args[0]))
                    using (var kdb = new KeyValueDB(dfc))
                    using (var tr = kdb.StartReadOnlyTransaction())
                    using (var st = File.Create(Path.Combine(args[0], "snapshot.dat")))
                    {
                        KeyValueDBExportImporter.Export(tr, st);
                    }

                    break;
                }
                case "import":
                {
                    using (var st = File.OpenRead(Path.Combine(args[0], "snapshot.dat")))
                    using (var dfc = new OnDiskFileCollection(args[0]))
                    using (var kdb = new KeyValueDB(dfc))
                    using (var tr = kdb.StartTransaction())
                    {
                        KeyValueDBExportImporter.Import(tr, st);
                        tr.Commit();
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

        class ConsoleKvdbLogger : IKeyValueDBLogger
        {
            public void ReportTransactionLeak(IKeyValueDBTransaction transaction)
            {
            }

            public void CompactionStart(ulong totalWaste)
            {
                Console.WriteLine($"Starting compaction with {totalWaste} wasted bytes");
            }

            public void CompactionCreatedPureValueFile(uint fileId, ulong size, uint itemsInMap, ulong roughMemory)
            {
                Console.WriteLine(
                    $"Pvl file {fileId} with size {size} created. Items in map {itemsInMap} roughly {roughMemory} bytes.");
            }

            public void KeyValueIndexCreated(uint fileId, long keyValueCount, ulong size, TimeSpan duration)
            {
                Console.WriteLine($"Kvi created {keyValueCount} keys with size {size} in {duration.TotalSeconds:F1}");
            }
        }
    }
}
