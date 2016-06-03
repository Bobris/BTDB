using System;
using System.Collections.Generic;
using System.Text;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest
{
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
            _db.Open(_lowDb, false);
        }

        public class Link
        {
            [PrimaryKey]
            public ulong Id { get; set; }
            public IDictionary<ulong, ulong> Edges { get; set; }
        }

        public interface ILinks
        {
            void Insert(Link link);
            bool RemoveById(ulong id);
        }

        [Fact]
        public void FreeIDictionary()
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
            AssertNoLeaksInDb();
            using (var tr = _db.StartTransaction())
            {
                var links = creator(tr);
                Assert.True(links.RemoveById(1));
                tr.Commit();
            }
            AssertNoLeaksInDb();
        }

        public class LinkInList
        {
            [PrimaryKey]
            public ulong Id { get; set; }
            public List<IDictionary<ulong, ulong>> EdgesList { get; set; }
        }

        public interface ILinksInList
        {
            void Insert(LinkInList link);
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
                        new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 } ,
                        new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 }
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


        public class LinkInDict
        {
            [PrimaryKey]
            public ulong Id { get; set; }
            public Dictionary<int, IDictionary<ulong, ulong>> EdgesIDict { get; set; }
            public string Name { get; set; }
        }

        public interface ILinksInDict
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
            [PrimaryKey]
            public ulong Id { get; set; }
            public IDictionary<int, IDictionary<ulong, ulong>> EdgesIDict { get; set; }
        }

        public interface ILinksInIDict
        {
            void Insert(LinkInIDict link);
            bool RemoveById(ulong id);
        }

        [Fact(Skip = "not finished")] //todo
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
            [PrimaryKey]
            public ulong Id { get; set; }
            public Nodes Nodes { get; set; }
        }

        public interface ILinksWithNodes
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


        void AssertNoLeaksInDb()
        {
            Assert.Equal("", FindLeaks());
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
}