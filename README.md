# BTDB

Currently this project these parts:

* Key Value Database
* Wrapped Dynamic IL generation with debugging + extensions
* IOC Container
* Object Database
* RPC Library
* Dto Channel
* Snappy Compression
* Event Storage

All code written in C# and licenced under very permissive [MIT licence](http://www.opensource.org/licenses/mit-license.html). Currently targeting .Net 4.5, main code is without any dependency. Code is tested using NUnit Framework.
Please is you find it useful or have questions, write me e-mail <boris.letocha@gmail.com> so I know that it is used.
It is available in Nuget <http://www.nuget.org/packages/BTDB>

---
## Key Value Database

### Features:

* This is Key Value store written in C# without using any native code.
* It is easily embeddable. 
* One storage is just one directory.
* It has [ACID] properties with [MVCC].
* At one time there could be multiple read only transactions and one read/write transaction.
* Export/Import to stream - could be used for compaction, snapshotting
* Automatic compaction
* Customizable compression
* Relatively Fast DB Open due to key index file
* Inspired by Bitcask [https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf]

### Design limits:

* All keys data needs to fit in RAM
* Maximum Key length is limited by 31bits (2GB).
* Maximum value length is limited by 31bits (2GB).

### Sample code:

    using (var fileCollection = new InMemoryFileCollection())
    using (IKeyValueDB db = new KeyValueDB(fileCollection))
    {
        using (var tr = db.StartTransaction())
        {
            tr.CreateOrUpdateKeyValue(new byte[] { 1 }, new byte[100000]);
            tr.Commit();
        }
    }

### Roadmap:

* Everything is there just use it

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

Documentation: [https://github.com/Bobris/BTDB/blob/master/Doc/ODBDictionary.md]

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

* Support more types of properties
* Performance tests
* Free text search (far future)

---
## RPC Library

Deprecated use Dto Channel instead (RPC is really too easy to abuse and get bad performance)

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

---
## Event storage

### Features:

* Optimal serialization with metadata
* Deserialization also to dynamic
* Storage is transactional
* As storage could be used Azure Page Blobs
 
---
## Dto Channel

### Features:

* Send and receive Dto over Tcp/Ip
* Identical serialization from Event Storage

### Sample code:

    object u1 = new User { Name = "A", Age = 1 };
    object u2 = null;
    _second.OnReceive.Subscribe(o => u2 = o);
    _first.Send(u1);

### Roadmap:

* Even more speed and event based TCP/IP server channels

---
## Snappy Compression

### Features:

* Ported and inspired mainly by Go version of Snappy Compression [http://code.google.com/p/snappy/]
* Fully compatible with original
* Fully managed and safe implementation
* Compression is aborted when target buffer size is not big enough

### Roadmap:

* Slower but better compressors options

[ACID]:http://en.wikipedia.org/wiki/ACID
[MVCC]:http://en.wikipedia.org/wiki/Multiversion_concurrency_control
