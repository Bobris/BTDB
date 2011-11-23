# BTDB

Currently this project has 3 main parts:

* Key Value Database
* Wrapped Dynamic IL generation with debugging + extensions
* Object Database
* RPC Library

All code written in C# and licenced under very permissive [MIT licence](http://www.opensource.org/licenses/mit-license.html). Currently targeting .Net 4.0, code uses Parallel Extensions. Code is tested using NUnit Framework.
Please is you find it useful or have questions, write me e-mail <boris.letocha@gmail.com> so I know that it is used.

---
## Key Value Database

### Features:

* This is Key Value store written in C# without using any native code.
* It is easily embeddable. 
* One storage is just one file/stream.
* It has [ACID] properties with [MVCC].
* At one time there could be multiple read only transactions and one read/write transaction.
* Because it reuses deallocated space, it does not need compaction (or at least not that often).
* Export/Import to stream - could be used for compaction

### Design limits:

* Maximum Key length is limited by 31bits (2GB). Best performance has keys with length smaller than 524 bytes.
* Maximum value length is limited by 63bits (8EB = 8 Exa Bytes).
* Total pairs count is limited by 63bits.
* Total size of database file is limited by 63bits (8EB).

### Sample code:

    using (var stream = new MemoryPositionLessStream())
    using (IKeyValueDB db = new KeyValueDB())
    {
        db.Open(stream, false);
        using (var tr = db.StartTransaction())
        {
            tr.CreateKey(new byte[] { 1 });
            tr.SetValue(new byte[100000]);
            tr.Commit();
        }
    }

### Roadmap:

* More tests, especially multi-threaded
* Bug fixing stabilization
* Transaction Log for better commit speed (hard and will be less important with SSD drives => low priority)

---
## Wrapped Dynamic IL generation with debugging + extensions

This help you to write fluent code which generates IL code in runtime. It is used in Object Database part.

### Sample code:

    var method = ILBuilder.Instance.NewMethod<Func<Nested>>("SampleCall");
    var il = method.Generator;
    var local = il.DeclareLocal(typeof(Nested), "n");
    il
        .Newobj(() => new Nested())
        .Dup()
        .Stloc(local)
        .Ldstr("Test")
        .Call(() => ((Nested)null).Fun(""))
        .Ldloc(local)
        .Ret();
    var action = method.Create();

### Roadmap:

* Add support for all IL instructions as needed

---
## Object Database

### Features:

* Builds on top of Key Value Database and Reflection.Emit extensions.
* It stores Plain .Net Objects and only their public properties with getters and setters.
* All [ACID] and [MVCC] properties are preserved of course.
* Automatic upgrading of model on read with dynamically generated optimal IL code.
* Automatic versioning of model changes.
* Enumeration of all objects
* Each object type could store its "singleton" - very useful for root objects

### Sample code:

    public class Person
    {
        public string Name { get; set; }
        public uint Age { get; set; }
    }

    using (var tr = _db.StartTransaction())
    {
        tr.Store(new Person { Name = "Bobris", Age = 35 });
        tr.Commit();
    }
    using (var tr = _db.StartTransaction())
    {
        var p = tr.Enumerate<Person>().First();
        Assert.AreEqual("Bobris", p.Name);
        Assert.AreEqual(35, p.Age);
    }

### Roadmap:

* Support more simple types of properties
* Performance tests
* Free text search (far future)

---
## RPC Library

### Features:

* TCP/IP comunication, service types negotiation
* Automatic serialization with dynamically generated optimal IL code.
* Both Client and Server can register services
* Async calls, OneWay calls, Exception propagation
* Services could be interfaces, classes, delegates

### Sample code:

    SimpleDTO received = null;
    _first.RegisterLocalService((Action<SimpleDTO>)(a => received = a));
    var d = _second.QueryRemoteService<Action<SimpleDTO>>();
    d(new SimpleDTO { Name = "Text", Number = 3.14 });
    Assert.NotNull(received);

### Roadmap:

* Even more speed and event based TCP/IP server channels

[ACID]:http://en.wikipedia.org/wiki/ACID
[MVCC]:http://en.wikipedia.org/wiki/Multiversion_concurrency_control
