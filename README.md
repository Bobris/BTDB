# BTDB

Currently this project these parts:

-   Key Value Database
-   Wrapped Dynamic IL generation with debugging + extensions
-   IOC Container
-   Object Database with Relations
-   Snappy Compression
-   Event Storage
-   Bon (Binary object notation)

All code written in C# 11 and licensed under very permissive [MIT license](http://www.opensource.org/licenses/mit-license.html). Targeting .Net 7.0, main code has just 1 dependency (Microsoft.Extensions.Primitives). Code is tested using xUnit Framework. Used in production on Windows and Linux, on OSX works as well.
Please is you find it useful or have questions, write me e-mail <boris.letocha@gmail.com> so I know that it is used.
It is available in Nuget <http://www.nuget.org/packages/BTDB>. Source code drops are Github releases.

---

## Key Value Database

### Features:

-   This is Key Value store written in C# with 2 implementation old on managed heap and new on native heap (has also prefix compression).
-   It is easily embeddable.
-   One storage is just one directory.
-   It has [ACID] properties with [MVCC].
-   At one time there could be multiple read only transactions and one read/write transaction.
-   Export/Import to stream - could be used for compaction, snapshotting
-   Automatic compaction
-   Customizable compression
-   Relatively Fast DB Open due to key index file - though it still needs to load all keys to memory
-   Inspired by Bitcask [https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf]

### Design limits:

-   All keys data needs to fit in RAM
-   Maximum Key length is limited by 31bits (2GB).
-   Maximum value length is limited by 31bits (2GB).

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

-   Everything is there just use it

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

-   Add support for all IL instructions as needed

---

## Object Database

### Features:

-   Builds on top of Key Value Database and Reflection.Emit extensions.
-   It stores Plain .Net Objects and only their public properties with getters and setters.
-   All [ACID] and [MVCC] properties are preserved of course.
-   Automatic upgrading of model on read with dynamically generated optimal IL code.
-   Automatic versioning of model changes.
-   Enumeration of all objects
-   Each object type could store its "singleton" - very useful for root objects
-   Relations - Table with primary key and multiple secondary keys
-   By default objects are stored inline in parent object, use IIndirect for objects with Oid which will load lazily

Documentation: [https://github.com/Bobris/BTDB/blob/master/Doc/ODBDictionary.md]

Relations doc: [https://github.com/Bobris/BTDB/blob/master/Doc/Relations.md]

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

-   Support more types of properties
-   Free text search (far future if ever)

---

## Event storage

### Features:

-   Optimal serialization with metadata
-   Deserialization also to dynamic
-   Storage is transactional
-   As storage could be used Azure Page Blobs
-   EventStorage2 is specialized to be used with Kafka, metadata are stored in separate topic

---

## Bon

`Bon` Binary object notation is allows creating and reading JavaScript/C# values with extensions like Dictionary/Map into binary notation. It is much faster to parse, write, skip, search by keys than JSON, size will be also smaller in most cases, in some cases much more smaller.

---

## Snappy Compression

### Features:

-   Ported and inspired mainly by Go version of Snappy Compression [http://code.google.com/p/snappy/]
-   Fully compatible with original
-   Fully managed and safe implementation
-   Compression is aborted when target buffer size is not big enough

### Roadmap:

-   Some speed optimizations around Spans would help

[acid]: http://en.wikipedia.org/wiki/ACID
[mvcc]: http://en.wikipedia.org/wiki/Multiversion_concurrency_control
