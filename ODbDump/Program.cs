using System;
using System.Collections.Generic;
using System.IO;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using System.Diagnostics;
using ODbDump.Visitor;
using BTDB.StreamLayer;
using System.Linq;
using System.Text;
using System.Threading;
using BTDB.Collections;
using Microsoft.Extensions.Primitives;
using ODbDump.TrlDump;

namespace ODbDump
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Need to have just one parameter with directory of ObjectDB");
                Console.WriteLine(
                    "Optional second parameter: nicedump, comparedump, diskdump, dump, dumpnull, stat, fileheaders, compact, export, import, leaks, leakscode, size, frequency, interactive, check, findsplitbrain, fulldiskdump, trldump");
                return;
            }

            var action = "nicedump";
            if (args.Length > 1)
            {
                action = args[1].ToLowerInvariant();
            }

            switch (action)
            {
                case "realpath":
                {
                    var res = PlatformMethods.Instance.RealPath(args[0]);
                    if (res == null)
                        Console.WriteLine("Error resolving real path for " + args[0]);
                    else
                        Console.WriteLine(res);
                    break;
                }
                case "nicedump":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var odb = new ObjectDB();
                    odb.Open(kdb, false);
                    using var trkv = kdb.StartReadOnlyTransaction();
                    using var tr = odb.StartTransaction();
                    Console.WriteLine("CommitUlong: " + tr.GetCommitUlong());
                    Console.WriteLine("Ulong[0] oid: " + trkv.GetUlong(0));
                    Console.WriteLine("Ulong[1] dictid: " + trkv.GetUlong(1));
                    var visitor = new ToConsoleVisitorNice();
                    var iterator = new ODBIterator(tr, visitor);
                    iterator.Iterate();

                    break;
                }
                case "interactive":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var odb = new ObjectDB();
                    odb.Open(kdb, false);
                    using var trkv = kdb.StartReadOnlyTransaction();
                    using var tr = odb.StartTransaction();
                    Console.WriteLine("CommitUlong: " + tr.GetCommitUlong());
                    Console.WriteLine("Ulong[0] oid: " + trkv.GetUlong(0));
                    Console.WriteLine("Ulong[1] dictid: " + trkv.GetUlong(1));
                    var visitor = new ToConsoleVisitorNice();
                    var iterator = new ODBIterator(tr, visitor);
                    iterator.LoadGlobalInfo(true);
                    Interactive(iterator, visitor);

                    break;
                }
                case "comparedump":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var odb = new ObjectDB();
                    odb.Open(kdb, false);
                    using var trkv = kdb.StartReadOnlyTransaction();
                    using var tr = odb.StartTransaction();
                    var visitor = new ToConsoleVisitorForComparison();
                    var iterator = new ODBIterator(tr, visitor);
                    iterator.Iterate(sortTableByNameAsc: true);

                    break;
                }
                case "comparesplitdump":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var odb = new ObjectDB();
                    odb.Open(kdb, false);
                    using var trkv = kdb.StartReadOnlyTransaction();
                    using var tr = odb.StartTransaction();
                    using var visitor = new ToFilesVisitorForComparison(HashType.Crc32);
                    var iterator = new ODBIterator(tr, visitor);
                    iterator.Iterate(sortTableByNameAsc: true);

                    break;
                }
                case "fulldiskdump":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var odb = new ObjectDB();
                    odb.Open(kdb, false);
                    using var trkv = kdb.StartReadOnlyTransaction();
                    using var tr = odb.StartTransaction();
                    using var visitor = new ToFilesVisitorWithSecondaryKeys();
                    var iterator = new ODBIterator(tr, visitor);
                    iterator.Iterate(sortTableByNameAsc: true);

                    break;
                }
                case "diskdump":
                {
                    var dbDir = args[0];
                    var openUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null;
                    var fileName = Path.Combine(dbDir, "dump.txt");

                    DiskDump(dbDir, fileName, openUpToCommitUlong);
                    break;
                }
                case "dump":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var odb = new ObjectDB();
                    odb.Open(kdb, false);
                    using var trkv = kdb.StartReadOnlyTransaction();
                    using var tr = odb.StartTransaction();
                    Console.WriteLine("CommitUlong: " + tr.GetCommitUlong());
                    Console.WriteLine("Ulong[0] oid: " + trkv.GetUlong(0));
                    Console.WriteLine("Ulong[1] dictid: " + trkv.GetUlong(1));
                    var visitor = new ToConsoleVisitor();
                    var iterator = new ODBIterator(tr, visitor);
                    iterator.Iterate();

                    break;
                }
                case "dumpnull":
                case "null":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var odb = new ObjectDB();
                    odb.Open(kdb, false);
                    using var tr = odb.StartTransaction();
                    var visitor = new ToNullVisitor();
                    var iterator = new ODBIterator(tr, visitor);
                    iterator.Iterate();

                    break;
                }
                case "check":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var transaction = kdb.StartReadOnlyTransaction();
                    var keyValueCount = transaction.GetKeyValueCount();
                    transaction.FindFirstKey(ReadOnlySpan<byte>.Empty);
                    for (long kv = 0; kv < keyValueCount; kv++)
                    {
                        transaction.GetKey();
                        transaction.GetValue();
                        transaction.FindNextKey(ReadOnlySpan<byte>.Empty);
                    }

                    break;
                }
                case "stat":
                {
                    var sw = new Stopwatch();
                    sw.Start();
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        Compression = new SnappyCompressionStrategy(),
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    sw.Stop();
                    Console.WriteLine(
                        $"Opened in {sw.Elapsed.TotalSeconds:F1}s Using {Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024}MB RAM");
                    Console.WriteLine(kdb.CalcStats());

                    break;
                }
                case "statm": // Stat but by old managed implementation
                {
                    var sw = new Stopwatch();
                    sw.Start();
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        Compression = new SnappyCompressionStrategy(),
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    sw.Stop();
                    Console.WriteLine(
                        $"Opened in {sw.Elapsed.TotalSeconds:F1}s Using {Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024}MB RAM");
                    Console.WriteLine(kdb.CalcStats());

                    break;
                }
                case "kvi":
                {
                    var sw = Stopwatch.StartNew();
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new BTreeKeyValueDB(dfc, new SnappyCompressionStrategy(), 100 * 1024 * 1024, null);
                    Console.WriteLine(
                        $"Opened in {sw.Elapsed.TotalSeconds:F1}s Using {Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024}MB RAM");
                    sw.Restart();
                    kdb.CreateKvi(CancellationToken.None);
                    Console.WriteLine(
                        $"Created kvi in {sw.Elapsed.TotalSeconds:F1}s Using {Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024}MB RAM");

                    break;
                }
                case "kvim": // Kvi but by old managed implementation
                {
                    var sw = Stopwatch.StartNew();
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(dfc, new SnappyCompressionStrategy(), 100 * 1024 * 1024, null);
                    Console.WriteLine(
                        $"Opened in {sw.Elapsed.TotalSeconds:F1}s Using {Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024}MB RAM");
                    sw.Restart();
                    kdb.CreateKvi(CancellationToken.None);
                    Console.WriteLine(
                        $"Created kvi in {sw.Elapsed.TotalSeconds:F1}s Using {Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024}MB RAM");

                    break;
                }
                case "fileheaders":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    var fcfi = new FileCollectionWithFileInfos(dfc);
                    foreach (var fi in fcfi.FileInfos)
                    {
                        var details = "";
                        switch (fi.Value)
                        {
                            case IKeyIndex keyIndex:
                            {
                                details =
                                    $"KVCount:{keyIndex.KeyValueCount} CommitUlong:{keyIndex.CommitUlong} TrLogFileId:{keyIndex.TrLogFileId} TrLogOffset:{keyIndex.TrLogOffset}\n";
                                details += LoadUsedFilesFromKvi(keyIndex, fcfi, fi.Key);
                                break;
                            }
                            case IFileTransactionLog trlog:
                                details = $"Previous File Id: {trlog.PreviousFileId}";
                                break;
                        }

                        Console.WriteLine("File {0} Guid:{3} Gen:{2} Type:{1} {4}", fi.Key,
                            fi.Value.FileType.ToString(), fi.Value.Generation, fi.Value.Guid, details);
                    }

                    break;
                }
                case "compact":
                {
                    var sw = new Stopwatch();
                    sw.Start();
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(dfc, new SnappyCompressionStrategy(), 100 * 1024 * 1024, null);
                    kdb.Logger = new ConsoleKvdbLogger();
                    sw.Stop();
                    Console.WriteLine(
                        $"Opened in {sw.Elapsed.TotalSeconds:F1}s Using {Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024}MB RAM");
                    sw.Restart();
                    while (kdb.Compact(new CancellationToken()))
                    {
                        sw.Stop();
                        Console.WriteLine($"Compaction iteration in {sw.Elapsed.TotalSeconds:F1}");
                        sw.Restart();
                    }

                    sw.Stop();
                    Console.WriteLine(
                        $"Final compaction in {sw.Elapsed.TotalSeconds:F1} Using {Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024}MB RAM");
                    Console.WriteLine(kdb.CalcStats());

                    break;
                }
                case "export":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        Compression = new SnappyCompressionStrategy(),
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var tr = kdb.StartReadOnlyTransaction();
                    using var st = File.Create(Path.Combine(args[0], "snapshot.dat"));
                    KeyValueDBExportImporter.Export(tr, st);

                    break;
                }
                case "import":
                {
                    using var st = File.OpenRead(Path.Combine(args[0], "snapshot.dat"));
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(dfc);
                    using var tr = kdb.StartTransaction();
                    KeyValueDBExportImporter.Import(tr, st);
                    tr.Commit();

                    break;
                }
                case "leaks":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        Compression = new SnappyCompressionStrategy(),
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var odb = new ObjectDB();
                    Console.WriteLine("Leaks: ");
                    odb.Open(kdb, false);
                    odb.DumpLeaks();

                    break;
                }
                case "leakscode":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        Compression = new SnappyCompressionStrategy(),
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var odb = new ObjectDB();
                    odb.Open(kdb, false);
                    odb.DumpLeaksCode();

                    break;
                }
                case "frequency":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        Compression = new SnappyCompressionStrategy(),
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var odb = new ObjectDB();
                    odb.Open(kdb, false);
                    using var tr = odb.StartTransaction();
                    var visitor = new ToConsoleFrequencyVisitor();
                    var iterator = new ODBIterator(tr, visitor);
                    iterator.Iterate();
                    visitor.OutputStatistic();
                }
                    break;
                case "size":
                {
                    using var dfc = new OnDiskFileCollection(args[0]);
                    using var kdb = new KeyValueDB(new KeyValueDBOptions
                    {
                        FileCollection = dfc,
                        ReadOnly = true,
                        Compression = new SnappyCompressionStrategy(),
                        OpenUpToCommitUlong = args.Length >= 3 ? (ulong?)ulong.Parse(args[2]) : null
                    });
                    using var odb = new ObjectDB();
                    odb.Open(kdb, false);
                    using var tr = odb.StartTransaction();
                    var visitor = new ToConsoleSizeVisitor();
                    var iterator = new ODBIterator(tr, visitor);
                    iterator.Iterate();
                }
                    break;
                case "findsplitbrain":
                {
                    if (args.Length != 6)
                    {
                        Console.WriteLine(
                            "usage: ODBDump Eagle_0 findsplitbrain Eagle_1 1000 100000 INestedEventTable");
                        return;
                    }

                    var startEvent = ulong.Parse(args[3]);
                    var endEvent = ulong.Parse(args[4]);
                    var relationName = args[5];

                    var (found, lastGood, firstBad) =
                        FindSplitBrain(args[0], args[2], relationName, startEvent, endEvent);
                    if (found)
                    {
                        Console.WriteLine($"Split occured between {lastGood} and {firstBad}");
                        DiskDump(args[0], $"dump_{lastGood}_0.txt", lastGood);
                        DiskDump(args[2], $"dump_{lastGood}_1.txt", lastGood);
                        DiskDump(args[0], $"dump_{firstBad}_0.txt", firstBad);
                        DiskDump(args[2], $"dump_{firstBad}_1.txt", firstBad);
                    }
                }
                    break;
                case "trldump":
                {
                    ITrlVisitor visitor = new ConsoleTrlVisitor();
                    using var dfc = new OnDiskFileCollection(args[0]);
                    foreach (var file in dfc.Enumerate())
                    {
                        try
                        {
                            visitor.StartFile(file.Index, file.GetSize());
                            var reader = new TrlFileReader(file);
                            reader.Iterate(visitor);
                        }
                        catch (Exception e)
                        {
                            visitor.OperationDetail("Failure " + e.Message);
                        }

                        visitor.EndOperation();
                    }
                }
                    break;
                default:
                {
                    Console.WriteLine($"Unknown action: {action}");
                    break;
                }
            }
        }

        static string LoadUsedFilesFromKvi(IKeyIndex keyIndex, FileCollectionWithFileInfos fcfi, uint fileId)
        {
            try
            {
                var file = fcfi.GetFile(fileId);
                var reader = new SpanReader(file.GetExclusiveReader());
                FileKeyIndex.SkipHeader(ref reader);
                var keyCount = keyIndex.KeyValueCount;
                var usedFileIds = new RefDictionary<uint, ulong>();
                if (keyIndex.Compression == KeyIndexCompression.Old)
                {
                    for (var i = 0; i < keyCount; i++)
                    {
                        var keyLength = reader.ReadVInt32();
                        reader.SkipBlock(keyLength);
                        var vFileId = reader.ReadVUInt32();
                        reader.SkipVUInt32();
                        var len = reader.ReadVInt32();
                        if (vFileId > 0) usedFileIds.GetOrAddValueRef(vFileId) += (ulong)Math.Abs(len);
                    }
                }
                else
                {
                    if (keyIndex.Compression != KeyIndexCompression.None)
                        return "";
                    for (var i = 0; i < keyCount; i++)
                    {
                        reader.SkipVUInt32();
                        var keyLengthWithoutPrefix = (int)reader.ReadVUInt32();
                        reader.SkipBlock(keyLengthWithoutPrefix);
                        var vFileId = reader.ReadVUInt32();
                        reader.SkipVUInt32();
                        var len = reader.ReadVInt32();
                        if (vFileId > 0) usedFileIds.GetOrAddValueRef(vFileId) += (ulong)Math.Abs(len);
                    }
                }

                var used = usedFileIds.OrderBy(a => a.Key).ToArray();
                var sb = new StringBuilder();
                foreach (var keyValuePair in used)
                {
                    sb.Append("    in ");
                    sb.Append(keyValuePair.Key);
                    sb.Append(" used ");
                    sb.Append(keyValuePair.Value);
                    sb.Append('\n');
                }
                return sb.ToString();
            }
            catch
            {
                // ignore
            }

            return "";
        }

        static (bool found, ulong lastGood, ulong firstBad) FindSplitBrain(string dir1, string dir2,
            string relationName, ulong startEvent, ulong endEvent)
        {
            if (!CheckStartEquals()) return (false, 0, 0);

            bool CheckStartEquals()
            {
                var startContent0 = DumpRelationContent(dir1, relationName, startEvent);
                var startContent1 = DumpRelationContent(dir2, relationName, startEvent);
                if (startContent0 == startContent1) return true;
                Console.WriteLine("DBs differs already on start event.");
                return false;
            }

            if (!CheckEndDiffers()) return (false, 0, 0);

            bool CheckEndDiffers()
            {
                var endContent0 = DumpRelationContent(dir1, relationName, endEvent);
                var endContent1 = DumpRelationContent(dir2, relationName, endEvent);
                if (endContent0 != endContent1) return true;
                Console.WriteLine("DBs are same on end event.");
                return false;
            }

            var l = startEvent;
            var r = endEvent;

            while (l + 1 < r)
            {
                var m = (r + l) / 2;
                var content0 = DumpRelationContent(dir1, relationName, m);
                var content1 = DumpRelationContent(dir2, relationName, m);
                if (content0 == content1)
                    l = m;
                else
                    r = m;
                Console.WriteLine($"Narrowing to {l} .. {r}");
            }

            return (true, l, r);
        }

        static string DumpRelationContent(string dbDir, string relationName, ulong? openUpToCommitUlong)
        {
            using var dfc = new OnDiskFileCollection(dbDir);
            using var kdb = new KeyValueDB(new KeyValueDBOptions
            {
                FileCollection = dfc,
                ReadOnly = true,
                OpenUpToCommitUlong = openUpToCommitUlong
            });
            using var odb = new ObjectDB();
            odb.Open(kdb, false);
            using var trkv = kdb.StartReadOnlyTransaction();
            using var tr = odb.StartTransaction();

            var visitor = new ToStringFastVisitor();
            var iterator = new ODBIterator(tr, visitor);
            iterator.LoadGlobalInfo();
            var relationIdInfo = iterator.RelationId2Info.FirstOrDefault(kvp => kvp.Value.Name.EndsWith(relationName));
            if (relationIdInfo.Value == null) return "";
            iterator.IterateRelation(relationIdInfo.Value);
            return visitor.ToString();
        }

        static void DiskDump(string dbDir, string fileName, ulong? openUpToCommitUlong)
        {
            using var dfc = new OnDiskFileCollection(dbDir);
            using var kdb = new KeyValueDB(new KeyValueDBOptions
            {
                FileCollection = dfc,
                ReadOnly = true,
                OpenUpToCommitUlong = openUpToCommitUlong
            });
            using var odb = new ObjectDB();
            using var tst = File.CreateText(fileName);
            odb.Open(kdb, false);
            using var trkv = kdb.StartReadOnlyTransaction();
            using var tr = odb.StartTransaction();
            tst.WriteLine("CommitUlong: " + tr.GetCommitUlong());
            tst.WriteLine("Ulong[0] oid: " + trkv.GetUlong(0));
            tst.WriteLine("Ulong[1] dictid: " + trkv.GetUlong(1));
            var visitor = new ToFileVisitorNice(tst);
            var iterator = new ODBIterator(tr, visitor);
            iterator.Iterate();
        }

        static void Interactive(ODBIterator iterator, ToConsoleVisitorNice visitor)
        {
            Console.WriteLine("Enter command:");
            var currentRelationId = -1;
            while (true)
            {
                var line = Console.ReadLine();
                if (line == null) break;
                var words = line.Split(' ');
                switch (words[0])
                {
                    default:
                        Console.WriteLine("Unknown command " + words[0]);
                        goto case "help";
                    case "":
                    case "h":
                    case "help":
                        Console.WriteLine("Commands help:");
                        Console.WriteLine("l list");
                        Console.WriteLine("e exit");
                        continue;
                    case "o":
                    case "oid":
                        if (words.Length == 2)
                        {
                            if (uint.TryParse(words[1], out var oid))
                                iterator.IterateOid(oid);
                        }
                        else
                        {
                            Console.WriteLine("oid command help:");
                            Console.WriteLine("oid id");
                            continue;
                        }

                        break;
                    case "s":
                    case "select":
                        if (words.Length == 1)
                        {
                            Console.WriteLine("Select command help:");
                            Console.WriteLine("select relation id");
                            continue;
                        }

                        if (long.TryParse(words[1], out var selectId))
                        {
                            if (currentRelationId >= 0)
                            {
                                iterator.IterateRelationRow(iterator.RelationId2Info[(uint)currentRelationId],
                                    selectId);
                            }

                            break;
                        }

                        switch (words[1])
                        {
                            case "r":
                            case "relation":
                                if (words.Length == 3)
                                {
                                    if (uint.TryParse(words[2], out var id) && iterator.RelationId2Info.ContainsKey(id))
                                    {
                                        currentRelationId = (int)id;
                                    }
                                }

                                break;
                        }

                        break;
                    case "l":
                    case "list":
                        if (words.Length == 1)
                        {
                            Console.WriteLine("List command help:");
                            Console.WriteLine("list relation");
                            Console.WriteLine("list nonempty relations");
                            continue;
                        }

                        switch (words[1])
                        {
                            case "r":
                            case "relation":
                                if (words.Length == 2)
                                {
                                    foreach (var rel in iterator.RelationId2Info.Values.OrderBy(r => r.Id))
                                    {
                                        Console.WriteLine(rel.Id + " " + rel.Name + " " + rel.RowCount);
                                    }
                                }

                                break;
                            case "n":
                            case "nonempty":
                                if (words.Length == 3)
                                {
                                    switch (words[2])
                                    {
                                        case "r":
                                        case "relations":
                                            foreach (var rel in iterator.RelationId2Info.Values
                                                .Where(r => r.RowCount > 0).OrderBy(r => r.RowCount))
                                            {
                                                Console.WriteLine(rel.Id + " " + rel.Name + " " + rel.RowCount);
                                            }

                                            break;
                                    }
                                }

                                break;
                            case "t":
                            case "table":
                                if (words.Length == 2)
                                {
                                    foreach (var (id, name) in iterator.TableId2Name)
                                    {
                                        Console.WriteLine(id + " " + name);
                                    }
                                }

                                break;
                        }

                        break;
                    case "e":
                    case "exit":
                        return;
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

            public void TransactionLogCreated(uint fileId)
            {
                Console.WriteLine($"Trl file {fileId} added to collection.");
            }

            public void FileMarkedForDelete(uint fileId)
            {
                Console.WriteLine($"File {fileId} marked for delete.");
            }

            public void LogWarning(string message)
            {
                Console.WriteLine("Warning: " + message);
            }
        }
    }
}
