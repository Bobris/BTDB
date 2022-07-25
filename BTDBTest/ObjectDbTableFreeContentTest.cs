using System;
using System.Collections.Generic;
using System.Text;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ObjectDbTableFreeContentTest : IDisposable
{
    IKeyValueDB _lowDb;
    IObjectDB _db;

    public ObjectDbTableFreeContentTest()
    {
        _lowDb = new InMemoryKeyValueDB();
        OpenDb();
    }

    void OpenDb()
    {
        _db = new ObjectDB();
        _db.Open(_lowDb, false, new DBOptions().WithoutAutoRegistration());
    }

    void ReopenDb()
    {
        _db.Dispose();
        OpenDb();
    }

    public class Link
    {
        [PrimaryKey] public ulong Id { get; set; }
        public IDictionary<ulong, ulong> Edges { get; set; }
    }

    public interface ILinks : IRelation<Link>
    {
        void Insert(Link link);
        void Update(Link link);
        void ShallowUpdate(Link link);
        bool ShallowUpsert(Link link);
        bool RemoveById(ulong id);
        bool ShallowRemoveById(ulong id);
        Link FindById(ulong id);
    }

    [Fact]
    public void FreeIDictionary()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            Assert.True(links.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void FreeIDictionaryByRemoveAll()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            links.RemoveAll();
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    Func<IObjectDBTransaction, ILinks> InitILinks()
    {
        Func<IObjectDBTransaction, ILinks> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinks>("LinksRelation");
            var links = creator(tr);
            var link = new Link { Id = 1, Edges = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 } };
            links.Insert(link);
            tr.Commit();
        }

        return creator;
    }

    [Fact]
    public void FreeIDictionaryInUpdate()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            links.Insert(new Link { Id = 2, Edges = new Dictionary<ulong, ulong> { [10] = 20 } });
            var link = new Link { Id = 1, Edges = new Dictionary<ulong, ulong>() };
            links.Update(link); //replace dict
            link = links.FindById(2);
            link.Edges.Add(20, 30);
            links.Update(link); //update dict, must not free
            link = links.FindById(2);
            Assert.Equal(2, link.Edges.Count);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void LeakingIDictionaryInShallowUpdate()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            links.Insert(new Link { Id = 2, Edges = new Dictionary<ulong, ulong> { [10] = 20 } });
            var link = new Link { Id = 1, Edges = new Dictionary<ulong, ulong>() };
            links.ShallowUpdate(link); //replace dict
            link = links.FindById(2);
            link.Edges.Add(20, 30);
            links.ShallowUpdate(link); //update dict, must not free
            link = links.FindById(2);
            Assert.Equal(2, link.Edges.Count);
            tr.Commit();
        }

        Assert.NotEmpty(FindLeaks());
    }

    [Fact]
    public void LeakingIDictionaryInShallowRemove()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            Assert.True(links.ShallowRemoveById(1)); //remove without free
            Assert.Equal(0, links.Count);
            tr.Commit();
        }

        Assert.NotEmpty(FindLeaks());
    }

    [Fact]
    public void ReuseIDictionaryAfterShallowRemove()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var value = links.FindById(1);
            links.ShallowRemoveById(1); //remove without free
            Assert.Equal(0, links.Count);
            links.Insert(value);
            Assert.Equal(3, value.Edges.Count);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void FreeIDictionaryInUpsert()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var link = new Link { Id = 1, Edges = new Dictionary<ulong, ulong>() };
            links.Upsert(link); //replace dict
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void LeakingIDictionaryInShallowUpsert()
    {
        var creator = InitILinks();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var link = new Link { Id = 1, Edges = new Dictionary<ulong, ulong>() };
            links.ShallowUpsert(link); //replace dict
            tr.Commit();
        }

        Assert.NotEmpty(FindLeaks());
    }

    public class LinkInList
    {
        [PrimaryKey] public ulong Id { get; set; }
        public List<IDictionary<ulong, ulong>> EdgesList { get; set; }
    }

    public interface ILinksInList : IRelation<LinkInList>
    {
        void Insert(LinkInList link);
        void Update(LinkInList link);
        LinkInList FindById(ulong id);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void FreeIDictionaryInList()
    {
        Func<IObjectDBTransaction, ILinksInList> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinksInList>("ListLinksRelation");
            var links = creator(tr);
            var link = new LinkInList
            {
                Id = 1,
                EdgesList = new List<IDictionary<ulong, ulong>>
                    {
                        new Dictionary<ulong, ulong> {[0] = 1, [1] = 2, [2] = 3},
                        new Dictionary<ulong, ulong> {[0] = 1, [1] = 2, [2] = 3}
                    }
            };
            links.Insert(link);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);

            Assert.True(links.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    [Fact]
    public void FreeIDictionaryInListInUpdate()
    {
        Func<IObjectDBTransaction, ILinksInList> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinksInList>("ListLinksRelation");
            var links = creator(tr);
            var link = new LinkInList
            {
                Id = 1,
                EdgesList = new List<IDictionary<ulong, ulong>>
                    {
                        new Dictionary<ulong, ulong> {[0] = 1, [1] = 2, [2] = 3},
                        new Dictionary<ulong, ulong> {[0] = 1, [1] = 2, [2] = 3}
                    }
            };
            links.Insert(link);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var link = links.FindById(1);
            for (int i = 0; i < 20; i++)
                link.EdgesList.Add(new Dictionary<ulong, ulong> { [10] = 20 });
            links.Update(link);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class LinkInDict
    {
        [PrimaryKey] public ulong Id { get; set; }
        public Dictionary<int, IDictionary<ulong, ulong>> EdgesIDict { get; set; }
        public string Name { get; set; }
    }

    public interface ILinksInDict : IRelation<LinkInDict>
    {
        void Insert(LinkInDict link);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void FreeIDictionaryInDictionary()
    {
        Func<IObjectDBTransaction, ILinksInDict> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinksInDict>("DictLinksRelation");
            var links = creator(tr);
            var link = new LinkInDict
            {
                Id = 1,
                EdgesIDict = new Dictionary<int, IDictionary<ulong, ulong>>
                {
                    [0] = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 },
                    [1] = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }
                }
            };
            links.Insert(link);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            Assert.True(links.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class LinkInIDict
    {
        [PrimaryKey] public ulong Id { get; set; }
        public IDictionary<int, IDictionary<ulong, ulong>> EdgesIDict { get; set; }
    }

    public interface ILinksInIDict : IRelation<LinkInIDict>
    {
        void Insert(LinkInIDict link);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void FreeIDictionaryInIDictionary()
    {
        Func<IObjectDBTransaction, ILinksInIDict> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinksInIDict>("IDictLinksRelation");
            var links = creator(tr);
            var link = new LinkInIDict
            {
                Id = 1,
                EdgesIDict = new Dictionary<int, IDictionary<ulong, ulong>>
                {
                    [0] = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 },
                    [1] = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }
                }
            };
            links.Insert(link);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            Assert.True(links.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class Nodes
    {
        public IDictionary<ulong, ulong> Edges { get; set; }
    }

    public class Links
    {
        [PrimaryKey] public ulong Id { get; set; }
        public Nodes Nodes { get; set; }
    }

    public interface ILinksWithNodes : IRelation<Links>
    {
        void Insert(Links link);
        Links FindByIdOrDefault(ulong id);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void FreeIDictionaryInInlineObject()
    {
        Func<IObjectDBTransaction, ILinksWithNodes> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILinksWithNodes>("IDictObjLinksRelation");
            var links = creator(tr);
            var link = new Links
            {
                Id = 1,
                Nodes = new Nodes { Edges = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 } }
            };
            links.Insert(link);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        using (var tr = _db.StartTransaction())
        {
            var links = creator(tr);
            var l = links.FindByIdOrDefault(1);
            Assert.Equal(2ul, l.Nodes.Edges[1]);
            Assert.True(links.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }


    public class BlobLocation
    {
        public string Name { get; set; }
    }

    public class LicenseFileDb
    {
        public string FileName { get; set; }
        public BlobLocation Location { get; set; }
    }

    public class LicenseDb
    {
        [PrimaryKey(1)] public ulong ItemId { get; set; }
        public LicenseFileDb LicenseFile { get; set; }
    }

    public interface ILicenseTable : IRelation<LicenseDb>
    {
        void Insert(LicenseDb license);
        void Update(LicenseDb license);
    }

    [Fact]
    public void DoNotCrashOnUnknownType()
    {
        Func<IObjectDBTransaction, ILicenseTable> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILicenseTable>("LicRel");
            var lics = creator(tr);
            var license = new LicenseDb { ItemId = 1 }; //no LicenseFileDb inserted
            lics.Insert(license);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var lics = creator(tr);
            var license = new LicenseDb { ItemId = 1 };
            lics.Update(license);

            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class License
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong UserId { get; set; }

        public IDictionary<ulong, IDictionary<ulong, ConcurrentFeatureItemInfo>> ConcurrentFeautureItemsSessions
        {
            get;
            set;
        }
    }

    public class ConcurrentFeatureItemInfo
    {
        public DateTime UsedFrom { get; set; }
    }

    public interface ILicenses : IRelation<License>
    {
        void Insert(License license);
        bool RemoveById(ulong companyId, ulong userId);
        int RemoveById(ulong companyId);
    }

    [Fact]
    public void AlsoFieldsInsideIDictionaryAreStoredInlineByDefault()
    {
        Func<IObjectDBTransaction, ILicenses> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILicenses>("LicenseRel");
            var lics = creator(tr);
            lics.Insert(new License());
            var license = new License
            {
                CompanyId = 1,
                UserId = 1,
                ConcurrentFeautureItemsSessions =
                    new Dictionary<ulong, IDictionary<ulong, ConcurrentFeatureItemInfo>>
                    {
                        [4] = new Dictionary<ulong, ConcurrentFeatureItemInfo>
                        { [2] = new ConcurrentFeatureItemInfo() }
                    }
            };
            lics.Insert(license);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILicenses>("LicenseRel");
            var lics = creator(tr);
            lics.RemoveById(0, 1);
            lics.RemoveById(1, 1);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }


    public class File
    {
        [PrimaryKey] public ulong Id { get; set; }

        public IIndirect<RawData> Data { get; set; }
    }

    public class RawData
    {
        public byte[] Data { get; set; }
        public IDictionary<ulong, ulong> Edges { get; set; }
    }

    public interface IHddRelation : IRelation<File>
    {
        void Insert(File file);
        void RemoveById(ulong id);
        File FindById(ulong id);
    }

    [Fact]
    public void IIndirectIsNotFreedAutomatically()
    {
        Func<IObjectDBTransaction, IHddRelation> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<IHddRelation>("HddRelation");
            var files = creator(tr);
            var file = new File
            {
                Id = 1,
                Data = new DBIndirect<RawData>(new RawData
                {
                    Data = new byte[] { 1, 2, 3 },
                    Edges = new Dictionary<ulong, ulong> { [10] = 20 }
                })
            };
            files.Insert(file);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var files = creator(tr);
            var file = files.FindById(1);
            Assert.Equal(file.Data.Value.Data, new byte[] { 1, 2, 3 });
            files.RemoveById(1);
            tr.Commit();
        }

        Assert.NotEmpty(FindLeaks());
    }

    [Fact]
    public void IIndirectMustBeFreedManually()
    {
        Func<IObjectDBTransaction, IHddRelation> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<IHddRelation>("HddRelation");
            var files = creator(tr);
            var file = new File
            {
                Id = 1,
                Data = new DBIndirect<RawData>(new RawData
                {
                    Data = new byte[] { 1, 2, 3 },
                    Edges = new Dictionary<ulong, ulong> { [10] = 20 }
                })
            };
            files.Insert(file);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var files = creator(tr);
            var file = files.FindById(1);
            Assert.Equal(file.Data.Value.Data, new byte[] { 1, 2, 3 });
            file.Data.Value.Edges.Clear();
            tr.Delete(file.Data);
            files.RemoveById(1);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class Setting
    {
        [PrimaryKey] public ulong Id { get; set; }
        public License License { get; set; }
    }

    public interface ISettings : IRelation<Setting>
    {
        void Insert(Setting license);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void PreferInlineIsTransferredThroughDBObject()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<ISettings>("SettingRel");
            var settings = creator(tr);
            var setting = new Setting
            {
                Id = 1,
                License = new License
                {
                    CompanyId = 1,
                    ConcurrentFeautureItemsSessions =
                        new Dictionary<ulong, IDictionary<ulong, ConcurrentFeatureItemInfo>>
                        {
                            [4] = new Dictionary<ulong, ConcurrentFeatureItemInfo>
                            { [2] = new ConcurrentFeatureItemInfo() }
                        }
                }
            };
            settings.Insert(setting);
            settings.RemoveById(1);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public interface INodes
    {
    }

    public class NodesA : INodes
    {
        public string F { get; set; }
        public IDictionary<ulong, ulong> A { get; set; }
    }

    public class NodesB : INodes
    {
        public IDictionary<ulong, ulong> B { get; set; }
        public string E { get; set; }
    }

    public class Graph
    {
        [PrimaryKey] public ulong Id { get; set; }
        public INodes Nodes { get; set; }
    }

    public interface IGraph : IRelation<Graph>
    {
        void Insert(Graph license);
        Graph FindById(ulong id);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void FreeWorksAlsoForDifferentSubObjects()
    {
        _db.RegisterType(typeof(NodesA));
        _db.RegisterType(typeof(NodesB));

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IGraph>("Graph");
            var table = creator(tr);
            var graph = new Graph
            {
                Id = 1,
                Nodes = new NodesA { A = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }, F = "f" }
            };
            table.Insert(graph);
            graph = new Graph
            {
                Id = 2,
                Nodes = new NodesB { B = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }, E = "e" }
            };
            table.Insert(graph);

            Assert.True(table.FindById(1).Nodes is NodesA);
            Assert.True(table.FindById(2).Nodes is NodesB);

            table.RemoveById(1);
            table.RemoveById(2);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class Component
    {
        public IList<Component> Children { get; set; }
        public IDictionary<string, string> Props { get; set; }
    }

    public class View
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public Component Component { get; set; }
    }

    public interface IViewTable : IRelation<View>
    {
        void Insert(View license);
        void RemoveById(ulong id);
    }

    [Fact]
    public void FreeWorksInRecursiveStructures()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IViewTable>("View");
            var table = creator(tr);
            table.Insert(new View
            {
                Id = 1,
                Component = new Component
                {
                    Children = new List<Component>
                        {
                            new Component {Props = new Dictionary<string, string> {["a"] = "A"}},
                            new Component {Props = new Dictionary<string, string> {["b"] = "B"}}
                        }
                }
            });
            table.RemoveById(1);
            AssertNoLeaksInDb();
        }
    }

    [Fact]
    public void FreeWorksTogetherWithRemoveByPrefix()
    {
        Func<IObjectDBTransaction, ILicenses> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILicenses>("LicenseRel2");
            var lics = creator(tr);
            lics.Insert(new License());
            var license = new License
            {
                CompanyId = 1,
                UserId = 1,
                ConcurrentFeautureItemsSessions =
                    new Dictionary<ulong, IDictionary<ulong, ConcurrentFeatureItemInfo>>
                    {
                        [4] = new Dictionary<ulong, ConcurrentFeatureItemInfo>
                        { [2] = new ConcurrentFeatureItemInfo() }
                    }
            };
            lics.Insert(license);
            tr.Commit();
        }

        AssertNoLeaksInDb();
        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<ILicenses>("LicenseRel2");
            var lics = creator(tr);
            Assert.Equal(1, lics.RemoveById(1));
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public abstract class UploadDataBase
    {
        public BlobLocation Location { get; set; }
        public string FileName { get; set; }
    }

    public class SharedImageFile : UploadDataBase
    {
        [PrimaryKey(1)]
        [SecondaryKey("CompanyId")]
        public ulong CompanyId { get; set; }

        [PrimaryKey(2)] public ulong Id { get; set; }

        public new BlobLocation Location
        {
            get => base.Location;
            set => base.Location = value;
        }

        public BlobLocation TempLocation { get; set; }

        public int Width { get; set; }
        public int Height { get; set; }

        public IDictionary<int, bool> Dict { get; set; }
    }

    public interface IFileTable : IRelation<SharedImageFile>
    {
        void Insert(SharedImageFile license);
        void RemoveById(ulong companyId, ulong id);
    }

    [Fact]
    public void IterateWellObjectsWithSharedInstance()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IFileTable>("IFileTable");
            var files = creator(tr);
            var loc = new BlobLocation();
            files.Insert(new SharedImageFile
            {
                Location = loc,
                TempLocation = loc,
                Dict = new Dictionary<int, bool> { [1] = true }
            });
            files.RemoveById(0, 0);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class ImportData
    {
        [PrimaryKey] public ulong CompanyId { get; set; }
        [PrimaryKey(Order = 1)] public ulong Id { get; set; }

        public IDictionary<ObjectId, ObjectNode> Items { get; set; }
    }

    public interface IImportDataTable : IRelation<ImportData>
    {
        bool Insert(ImportData item);
        void Update(ImportData item);
        int RemoveById(ulong companyId);
    }

    public class ObjectNode
    {
        public string Sample { get; set; }
    }

    public class ObjectId
    {
        public ulong Id { get; set; }
    }

    [Fact]
    public void DoNotPanicWhenUnknownStatusInIDictionaryKey()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IImportDataTable>("ImportData");
            var table = creator(tr);
            table.Insert(new ImportData
            {
                Items = new Dictionary<ObjectId, ObjectNode>
                {
                    [new ObjectId()] = new ObjectNode()
                }
            });
            tr.Commit();
        }
    }

    public class NodesBase
    {
    }

    public class NodesOne : NodesBase
    {
        public string F { get; set; }
        public IDictionary<ulong, ulong> A { get; set; }
    }

    public class NodesTwo : NodesBase
    {
        public IDictionary<ulong, ulong> B { get; set; }
        public string E { get; set; }
    }

    public class NodesGraph
    {
        [PrimaryKey] public ulong Id { get; set; }
        public NodesBase Nodes { get; set; }
    }

    public interface IGraphTable : IRelation<NodesGraph>
    {
        void Insert(NodesGraph license);
        NodesGraph FindById(ulong id);
        bool RemoveById(ulong id);
    }

    [Fact]
    public void FreeWorksAlsoForDifferentSubObjectsWithoutIface()
    {
        _db.RegisterType(typeof(NodesOne));
        _db.RegisterType(typeof(NodesTwo));

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IGraphTable>("GraphTable");
            var table = creator(tr);
            var graph = new NodesGraph
            {
                Id = 1,
                Nodes = new NodesOne { A = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }, F = "f" }
            };
            table.Insert(graph);
            graph = new NodesGraph
            {
                Id = 2,
                Nodes = new NodesTwo { B = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }, E = "e" }
            };
            table.Insert(graph);
            graph = new NodesGraph
            {
                Id = 3,
                Nodes = new NodesBase()
            };
            table.Insert(graph);

            Assert.True(table.FindById(1).Nodes is NodesOne);
            Assert.True(table.FindById(2).Nodes is NodesTwo);

            table.RemoveById(1);
            table.RemoveById(2);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    public class EmailMessage
    {
        public IDictionary<string, string> Bcc { get; set; }
        public IDictionary<string, string> Cc { get; set; }
        public IDictionary<string, string> To { get; set; }
        public IOrderedSet<string> Tags { get; set; }
    }

    public class EmailDb
    {
        public EmailMessage Content { get; set; }
    }

    public class BatchDb
    {
        [PrimaryKey(1)] public Guid ItemId { get; set; }
        public IDictionary<Guid, EmailDb> MailPieces { get; set; }
    }

    public interface IBatchTable : IRelation<BatchDb>
    {
        void Insert(BatchDb batch);
        void Update(BatchDb batch);
        BatchDb FindByIdOrDefault(Guid itemId);
    }

    [Fact(Skip = "prepared for discussion")]
    public void LeakCanBeMade()
    {
        Func<IObjectDBTransaction, IBatchTable> creator = null;
        var guid = Guid.NewGuid();
        var mailGuid = Guid.NewGuid();

        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<IBatchTable>("IBatchTable");
            var table = creator(tr);
            var batch = new BatchDb
            {
                ItemId = guid,
                MailPieces = new Dictionary<Guid, EmailDb>
                {
                    [mailGuid] = new EmailDb
                    {
                        Content = new EmailMessage
                        {
                            Bcc = new Dictionary<string, string> { ["a"] = "b" },
                            Cc = new Dictionary<string, string> { ["c"] = "d" },
                            To = new Dictionary<string, string> { ["e"] = "f" }
                        }
                    }
                }
            };
            table.Insert(batch);
            batch = table.FindByIdOrDefault(guid);
            batch.MailPieces[mailGuid].Content.Tags.Add("Important");
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var table = creator(tr);
            var batch = table.FindByIdOrDefault(guid);
            batch.MailPieces[mailGuid] =
                null; //LEAK - removed immediately from db, in table.Update don't have previous value
            table.Update(batch);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    void AssertNoLeaksInDb()
    {
        var leaks = FindLeaks();
        Assert.Equal("", leaks);
    }

    string FindLeaks()
    {
        using (var visitor = new FindUnusedKeysVisitor())
        {
            using (var tr = _db.StartReadOnlyTransaction())
            {
                visitor.ImportAllKeys(tr);
                visitor.Iterate(tr);
                return DumpUnseenKeys(visitor, " ");
            }
        }
    }

    static string DumpUnseenKeys(FindUnusedKeysVisitor visitor, string concat)
    {
        var builder = new StringBuilder();
        foreach (var unseenKey in visitor.UnseenKeys())
        {
            if (builder.Length > 0)
                builder.Append(concat);
            foreach (var b in unseenKey.Key)
                builder.Append(b.ToString("X2"));
            builder.Append(" Value len:");
            builder.Append(unseenKey.ValueSize);
        }

        return builder.ToString();
    }

    public void Dispose()
    {
        _db.Dispose();
        _lowDb.Dispose();
    }
}
