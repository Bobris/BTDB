using System;
using System.Collections.Generic;
using System.Diagnostics;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDBTest;

namespace SimpleTester
{
    public class RelationSpeedTest
    {
        public class Person
        {
            [PrimaryKey(1)]
            public ulong Id { get; set; }
            [SecondaryKey("Email")]
            public string Email { get; set; }
        }

        static ObjectDB CreateInMemoryDb()
        {
            var lowDb = new InMemoryKeyValueDB();
            var db = new ObjectDB();
            db.Open(lowDb, true);
            return db;
        }

        static ObjectDB CreateDb(IFileCollection fc)
        {
            var lowDb = new KeyValueDB(fc);
            var db = new ObjectDB();
            db.Open(lowDb, true);
            return db;
        }

        static IEnumerable<string> GenerateEmails(int count)
        {
            var rnd = new Random();
            while (count-- > 0)
                yield return $"a{rnd.Next(999)}@b{count}.com";
        }

        interface IMeasureOperations
        {
            void Insert();
            void List();
            void Update();
            void Remove();
        }

        //definitions for relations
        public interface IPersonTable : IRelation<Person>
        {
            void Insert(Person person);
            void Update(Person person);
            Person FindById(ulong id);
            void RemoveById(ulong id);
            IEnumerator<Person> ListByEmail(AdvancedEnumeratorParam<string> param);
        }
        //

        class RelationPersonTest : IMeasureOperations
        {
            readonly int _count;
            readonly ObjectDB _db;
            readonly Func<IObjectDBTransaction, IPersonTable> _creator;

            public RelationPersonTest(ObjectDB db, int count)
            {
                _count = count;
                _db = db;
                using var tr = _db.StartTransaction();
                _creator = tr.InitRelation<IPersonTable>("Job");
                tr.Commit();
            }

            public void Insert()
            {
                ulong idx = 0;
                foreach (var email in GenerateEmails(_count))
                {
                    using var tr = _db.StartTransaction();
                    var personTable = _creator(tr);
                    personTable.Insert(new Person { Email = email, Id = idx++ });
                    tr.Commit();
                }
            }

            public void List()
            {
                ulong sum = 0;
                using var tr = _db.StartTransaction();
                var personTable = _creator(tr);
                var en = personTable.ListByEmail(AdvancedEnumeratorParam<string>.Instance);
                while (en.MoveNext())
                {
                    sum += en.Current.Id;
                }
            }

            public void Update()
            {
                for (var id = 0ul; id < (ulong)_count; id++)
                {
                    using var tr = _db.StartTransaction();
                    var personTable = _creator(tr);
                    var p = personTable.FindById(id);
                    p.Email += "a";
                    personTable.Update(p);
                    tr.Commit();
                }
            }

            public void Remove()
            {
                for (var id = 0ul; id < (ulong)_count; id++)
                {
                    using var tr = _db.StartTransaction();
                    var personTable = _creator(tr);
                    personTable.RemoveById(id);
                    tr.Commit();
                }
            }
        }

        //definitions for singletons
        public class PersonMap
        {
            public IDictionary<ulong, Person> Items { get; set; }
        }

        public class PersonNameIndex
        {
            public IOrderedDictionary<string, ulong> Items { get; set; }
        }
        //

        class SingletonPersonTest : IMeasureOperations
        {
            readonly int _count;
            readonly ObjectDB _db;

            public SingletonPersonTest(ObjectDB db, int count)
            {
                _db = db;
                _count = count;
            }

            public void Insert()
            {
                ulong idx = 0;
                foreach (var email in GenerateEmails(_count))
                {
                    using var tr = _db.StartTransaction();
                    var persons = tr.Singleton<PersonMap>();
                    var emails = tr.Singleton<PersonNameIndex>();

                    persons.Items[idx] = new Person { Email = email, Id = idx };
                    emails.Items[email] = idx;
                    idx++;
                    tr.Commit();
                }
            }

            public void List()
            {
                using var tr = _db.StartReadOnlyTransaction();
                var persons = tr.Singleton<PersonMap>();
                var emails = tr.Singleton<PersonNameIndex>();
                ulong sum = 0;
                foreach (var e in emails.Items)
                {
                    var p = persons.Items[e.Value];
                    sum += p.Id;
                }
            }

            public void Update()
            {
                for (var id = 0ul; id < (ulong)_count; id++)
                {
                    using var tr = _db.StartTransaction();
                    var persons = tr.Singleton<PersonMap>();
                    var emails = tr.Singleton<PersonNameIndex>();

                    var p = persons.Items[id];
                    emails.Items.Remove(p.Email);
                    p.Email += "a";
                    emails.Items[p.Email] = id;
                    persons.Items[id] = p;
                    tr.Commit();
                }
            }

            public void Remove()
            {
                for (var id = 0ul; id < (ulong)_count; id++)
                {
                    using var tr = _db.StartTransaction();
                    var persons = tr.Singleton<PersonMap>();
                    var emails = tr.Singleton<PersonNameIndex>();

                    var p = persons.Items[id];
                    emails.Items.Remove(p.Email);
                    persons.Items.Remove(id);
                    tr.Commit();
                }
            }

            public void Dispose()
            {
                _db.Dispose();
            }
        }


        void Measure(string name, IMeasureOperations testImpl)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();

            Console.WriteLine($"Measure {name}");
            var sw = new Stopwatch();
            sw.Start();

            Do("Insert", testImpl.Insert);
            Do("Update", testImpl.Update);
            Do("List  ", testImpl.List);
            Do("Remove", testImpl.Remove);

            sw.Stop();

            Console.WriteLine($"Total : {TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds)}");
            Console.WriteLine();
        }

        void Do(string operationName, Action action)
        {
            var sw = new Stopwatch();
            sw.Start();
            action();
            sw.Stop();
            Console.WriteLine($"{operationName}: {TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds)}");
        }

        public void Run()
        {
            var cnt = 500000;
            using (var fc = new InMemoryFileCollection())
            using (var db = CreateDb(fc))
            {
                Measure("Relation: ", new RelationPersonTest(db, cnt));
            }
            using (var fc = new InMemoryFileCollection())
            using (var db = CreateDb(fc))
            {
                Measure("2 Maps: ", new SingletonPersonTest(db, cnt));
            }
            using (var db = CreateInMemoryDb())
            {
                Measure("Relation (mem): ", new RelationPersonTest(db, cnt));
            }
            using (var db = CreateInMemoryDb())
            {
                Measure("2 Maps (mem): ", new SingletonPersonTest(db, cnt));
            }
        }
    }
}
