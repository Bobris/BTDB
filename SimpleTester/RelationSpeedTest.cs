using System;
using System.Collections.Generic;
using System.Diagnostics;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

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

        ObjectDB CreateDb()
        {
            var lowDb = new InMemoryKeyValueDB();
            var db = new ObjectDB();
            db.Open(lowDb, true);
            return db;
        }

        IEnumerable<string> GenerateEmails(int count)
        {
            var rnd = new Random();
            while (count-- > 0)
                yield return $"a{rnd.Next(999)}@b{count}.com";
        }

        //definitinos for relations
        public interface IPersonTable
        {
            void Insert(Person person);
            IEnumerator<Person> ListByEmail(AdvancedEnumeratorParam<Person> param);
        }
        //

        void InsertPersonsAndIterateByEmailRelation(int count)
        {
            using (var db = CreateDb())
            {
                Func<IObjectDBTransaction, IPersonTable> creator;
                using (var tr = db.StartTransaction())
                {
                    creator = tr.InitRelation<IPersonTable>("Job");
                    var jobTable = creator(tr);
                    ulong idx = 0;
                    foreach (var email in GenerateEmails(count))
                    {
                        jobTable.Insert(new Person { Email = email, Id = idx++ });
                    }
                    tr.Commit();
                }
                ulong sum = 0;
                using (var tr = db.StartTransaction())
                {
                    var jobTable = creator(tr);
                    var en = jobTable.ListByEmail(new AdvancedEnumeratorParam<Person>());
                    while (en.MoveNext())
                    {
                        sum += en.Current.Id;
                    }
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

        void InsertPersonsAndIterateByEmailRelationSingleton(int count)
        {
            using (var db = CreateDb())
            {
                using (var tr = db.StartTransaction())
                {
                    var persons = tr.Singleton<PersonMap>();
                    var emails = tr.Singleton<PersonNameIndex>();
                    ulong idx = 0;
                    foreach (var email in GenerateEmails(count))
                    {
                        persons.Items[idx] = new Person { Email = email, Id = idx };
                        emails.Items[email] = idx;
                        idx++;
                    }
                    tr.Commit();
                }
                using (var tr = db.StartReadOnlyTransaction())
                {
                    var persons = tr.Singleton<PersonMap>();
                    var emails = tr.Singleton<PersonNameIndex>();
                    ulong sum = 0;
                    foreach (var e in emails.Items)
                    {
                        var p = persons.Items[e.Value];
                        sum += p.Id;
                    }
                }
            }
        }

        public void Run()
        {
            var sw = new Stopwatch();
            sw.Start();
            InsertPersonsAndIterateByEmailRelationSingleton(1000000);
            sw.Stop();
            Console.WriteLine($"2 Maps: {TimeSpan.FromTicks(sw.ElapsedTicks)}");

            GC.Collect();
            GC.WaitForPendingFinalizers();

            sw.Reset();
            sw.Start();
            InsertPersonsAndIterateByEmailRelation(1000000);
            sw.Stop();
            Console.WriteLine($"Relation: {TimeSpan.FromTicks(sw.ElapsedTicks)}");
        }
    }
}