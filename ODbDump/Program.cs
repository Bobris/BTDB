using System;
using System.IO;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using System.Diagnostics;
using ODbDump.Visitor;
using BTDB.StreamLayer;

namespace ODbDump
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Need to have just one parameter with directory of ObjectDB");
                Console.WriteLine("Optional second parameter: nicedump, comparedump, diskdump, dump, dumpnull, stat, fileheaders, compact, export, import, leaks, frequency");
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
                case "comparedump":
                {
                    using (var dfc = new OnDiskFileCollection(args[0]))
                    using (var kdb = new KeyValueDB(dfc))
                    using (var odb = new ObjectDB())
                    {
                        odb.Open(kdb, false);
                        using (var trkv = kdb.StartReadOnlyTransaction())
                        using (var tr = odb.StartTransaction())
                        {
                            var visitor = new ToConsoleVisitorForComparison();
                            var iterator = new ODBIterator(tr, visitor);
                            iterator.Iterate(sortTableByNameAsc: true);
                        }
                    }

                    break;
                }
                case "comparesplitdump":
                {
                    using (var dfc = new OnDiskFileCollection(args[0]))
                    using (var kdb = new KeyValueDB(dfc))
                    using (var odb = new ObjectDB())
                    {
                        odb.Open(kdb, false);
                        using (var trkv = kdb.StartReadOnlyTransaction())
                        using (var tr = odb.StartTransaction())
                        {
                            var visitor = new ToFilesVisitorForComparison(HashType.Crc32);
                            var iterator = new ODBIterator(tr, visitor);
                            iterator.Iterate(sortTableByNameAsc: true);
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
                        Console.WriteLine($"Opened in {sw.Elapsed.TotalSeconds:F1} Memory {GC.GetTotalMemory(true)} Working set {Process.GetCurrentProcess().WorkingSet64}");
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
                case "leaks":
                {
                    using (var dfc = new OnDiskFileCollection(args[0]))
                    using (var kdb = new KeyValueDB(dfc))
                    using (var odb = new ObjectDB())
                    {
                        Console.WriteLine("Leaks: ");
                        odb.Open(kdb, false);
                        odb.DumpLeaks();
                    }
                    break;
                }
                case "frequency":
                {
                    using (var dfc = new OnDiskFileCollection(args[0]))
                    using (var kdb = new KeyValueDB(dfc))
                    using (var odb = new ObjectDB())
                    {
                        odb.Open(kdb, false);
                        using (var tr = odb.StartTransaction())
                        {
                            var visitor = new FrequencyVisitor();
                            var iterator = new ODBIterator(tr, visitor);
                            iterator.Iterate();
                            visitor.OutputStatistic();
                        }
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
