# Relations

Relations provides easy way how to store "table" like data in object db.

Let's first define data entity we want to store (note that it is not defined as `[StoredInline]`, but it is still stored
inline). Such objects also don't have object id, they can be retrieved by primary or secondary indexes.

```C#
    public class Person
    {
        [PrimaryKey(1)]
        public ulong Id { get; set; }
        public string Name { get; set; }
        [SecondaryKey("Age")]
        public ulong Age { get; set; }
    }

    public interface IPersonTable : IRelation<Person>
    {
        void Insert(Person person);
        bool RemoveById(ulong id);
        Person FindById(ulong id);
        bool UpdateById(ulong id, string name);
    }
```

How do we get `IPersonTable` interface to actually insert persons? First we need [obtain transaction](ODBDictionary.md)

From transaction, we get creator of relation which we should keep and use for creating relation interface for
transaction
every time we need it.

```C#
    Func<IObjectDBTransaction, IPersonTable> creator;
    using (var tr = _db.StartTransaction())
    {
        creator = tr.InitRelation<IPersonTable>("Person");
        var personTable = creator(tr);
        personTable.Insert(new Person { Id = 2, Name = "admin", Age = 100 });
        tr.Commit();
    }
```

next time we reuse creator:

```C#
    using (var tr = _db.StartTransaction())
    {
        var personTable = creator(tr);
        return personTable.FindById(100);
    }
```

`InitRelation` can be called only once.

But this was "old school" way easier to just always use `GetRelation<T>`:

```C#
    using (var tr = _db.StartTransaction())
    {
        var personTable = tr.GetRelation<IPersonTable>();
        personTable.Insert(new Person { Id = 2, Name = "admin", Age = 100 });
        tr.Commit();
    }
```

## Initialize relations at startup

After opening ObjectDB, register the complete list of relation interfaces before starting application transactions:

```C#
    IObjectDB db = new ObjectDB();
    db.Open(keyValueDb, false, new DBOptions().WithoutAutoRegistrationOfRelations());
    await db.InitializeRelations(new[] { typeof(IPersonTable), typeof(IJobTable) });

    using var tr = db.StartReadOnlyTransaction();
    var people = tr.GetRelation<IPersonTable>();
```

The `IEnumerable<Type>` argument can be the list collected by an application's relation/IOC registration code
(such as Skymamba's continent relation list). `InitializeRelations` works with automatic registration disabled and
does not change that setting. Relation names come from `PersistedNameAttribute` on the interface, or otherwise its
type name, just like `GetRelation<T>`.

Initialization first checks the schemas and secondary-index counts in one read-only transaction. If any changes
are needed, it opens one writing transaction for the entire list and commits once. This persists schemas for new
relations even when they have no rows, upgrades existing schemas, repairs inconsistent secondary-index counts,
and invokes `IRelationOnCreate<T>` for new relations. All schemas and indexes are ready before creation callbacks
run, so callbacks can access other registered relations through the same transaction. The startup transaction
preserves the existing commit ulong/event ID.

If all schemas and indexes already match, no writing transaction is opened. Duplicate interface types are ignored;
different interfaces claiming the same persisted name are rejected. Already registered factories (including custom
factories registered with `RegisterCustomRelation`) are preserved and skipped. Register custom factories before
calling this method; include all ordinary relation interfaces in the startup list.

Call this method before other transactions or concurrent registration. A failed initialization rolls back its database
changes and discards the new registrations, allowing a retry. Creation callbacks must keep database writes inside
the supplied transaction; external side effects cannot be rolled back. Await initialization before using the database.

The older transaction-level `InitRelation` and automatic `GetRelation<T>` paths remain available. Creating a new
relation or upgrading its schema requires a writable transaction and persists its schema immediately, even with no
rows. Commit that transaction before reusing its relation factory. Rolling it back discards the new registration;
initialize again in a new transaction. Read-only transactions can initialize only relations whose persisted schemas
and indexes already match. Row writes and `AllocateId` never create relation schemas.

## Basic operations

When defined in interface following methods are automatically implemented by BTDB when defined in relation interface

### Insert

    (void|bool) personTable.Insert(new Person { Id = 2, Name = "admin", Age = 100 });

**void** variant will throw if already exists

**bool** variant returns true when inserted, false when already exists (without changing value)

### Update

    personTable.Update(new Person { Id = 2, Name = "superadmin", Age = 100 });

will throw if does not exist

### UpdateById

    (void|bool) UpdateById(primaryKey1, ..., primaryKeyN, valueField1, ..., valueFieldN)

Faster update but can change only nonprimary fields (or InKeyValues) and must be simple types (no classes, IIndirect<T>,
IDictionary<K,V>, IOrderedSet<T>). Value properties could be any number and in any order by they must match parameter
name case insensitively. Returns true if found and updated, void variant throw when does not exists. Any suffix could be
appended to function name `UpdateById`, use to disambiguate when multiple overrides would have same types of value sets.
All primary keys including InKeyValue must be always specified, if no additional value fields are specified update is
only in memory and does not read or writes value.

### Upsert (Insert or Update)

    var inserted = personTable.Upsert(new Person { Id = 2, Name = "superadmin", Age = 100 });

returns true if inserted, false if updated

Note: `Upsert` is automatically always available if you inherit from `IRelation<T>`

### ShallowUpdate

It is like `Update`, but it does not try to compare and free any nested content. It is especially faster without
secondary indexes when it does not even need to read old value.

### ShallowUpsert

It is like `Upsert`, but it does not try to compare and free any nested content. It is especially faster without
secondary indexes when it does not even need to read old value.

### UpsertRange

    (long Inserted, long Updated) UpsertRange(IEnumerable<T> items)

Every relation have this method it is very useful with `As<T>()` when you want quickly upgrade old relation into new
one.
Other than that it is pretty self-explanatory.

### As

    IEnumerable<T> As<T>()

Returns list of all rows in relation deserialized into `T` class. It should not be used often because it does not cache
generated deserialization code, its purpose is upgrade scenarios (See OnCreate below).

### Remove

    (void|bool) RemoveById(primaryKey1, ..., primaryKeyN);
    (void|bool) ShallowRemoveById(primaryKey1, ..., primaryKeyN);

Returns true if removed, void variant throw when does not exist. All primary keys fields are used as parameters, for
example `void RemoveById(ulong tenantId, ulong userId);`
ShallowRemoveById does not free nested content (like `IDictionary<K,V>`).

    int RemoveById(primaryKey1 [, primaryKey2, ...]);

Returns number of records removed for given primary key prefix for example `int RemoveById(ulong tenantId)` removes all
users for given tenant

    int RemoveByIdPartial(primaryKey1 [, primaryKey2, ...] , maxCount);

additionally can be limited number of deleted items at once

```C#
    int RemoveByIdPartial(ulong tenantId, int maxCount)
```

advanced enumeration param can be used same way as in ListById

```C#
    int RemoveById(primaryKey1, primaryKey2, ..., primaryKey_N-1, AdvancedEnumeratorParam<typeof(primaryKeyField(N))>);
```

### RemoveAll

    void RemoveAll();

This method is always available, because it is defined already in `IRelation`. It is even faster than RemoveById().

### Contains

    bool Contains(primaryKey1, ..., primaryKeyN);

Returns true if exist item with given primary key. All primary keys fields are used as parameters, for
example `bool Contains(ulong tenantId, ulong userId);`

### Find

    Person FindById(ulong id);

It will throw if does not exists, as parameters expects primary key fields (same as in RemoveById)

    Person FindByIdOrDefault(ulong id);

Will return null if not exists

    IEnumerable<T> FindById(primaryKey1 [, primaryKey2, ...]);

Find all items with given primary key prefix

    Person FindByAgeOrDefault(uint age);

Find by secondary key, it will throw if it finds multiple Persons with that age.

**Note**: "Age" in the name is name of secondary key index.

    IEnumerable<Person> FindByAge(uint age);

Find all items with given secondary key. **Note**: for advanced range enumerating use ListBy{SecondaryIndexName},
multiple result possibility handles legal case when exists several records for one secondary index key.

Find support returning also not item type but any subset type, but because you cannot have same name of method which
differs only by return type you can append any text to make it unique. This is useful for speed up deserialization
because only fields with matching names and types will be deserialized. Note: This feature is also called Variants.

    public class Age
    {
        public uint Age { get; set; }
    }

    IEnumerable<Age> FindByIdJustAge(ulong id);

### Iterate

`IterateById` synchronously visits all rows with the given primary-key prefix without allocating a result object for
each row. The final two parameters must be an `Action<T>` callback and a caller-provided `T` instance. BTDB loads each
matching row into that same instance before invoking the callback.

```C#
    public class UserRole
    {
        public ulong UserId { get; set; }
        public ulong RoleId { get; set; }
        [NotStored] public HashSet<ulong> ActiveUsers { get; set; } = null!;
    }

    void IterateById(ulong companyId, Action<UserRole> callback, UserRole value);
```

The reusable class can contain primary-key fields and stored value fields needed by the callback. Projections with
only primary-key fields avoid reading the stored value; other projections deserialize it using the stored schema version.
`[NotStored]` properties and fields are left untouched, so they can carry context into the callback.
The callback must not retain the row as a snapshot:
the same instance is overwritten by the next iteration. The callback must not modify the database during iteration.

### List

    IOrderedDictionaryEnumerator<uint, Person> ListById(AdvancedEnumeratorParam<uint> param);
    IEnumerable<Person> ListById(AdvancedEnumeratorParam<uint> param);
    ICollection<Person> ListById2(AdvancedEnumeratorParam<uint> param);
    IReadOnlyCollection<Person> ListById3(AdvancedEnumeratorParam<uint> param);
    IEnumerable<Person> ListById();
    ICollection<Person> ListById2();
    IReadOnlyCollection<Person> ListById3();

List by ascending/descending order and specified range. Parts of primary key may be used for listing. In example below
you can list all rooms or just rooms for specified company by two `ListById` method. (`IOrderedDictionaryEnumerator`,
`IEnumerable`, `ICollection`, or `IReadOnlyCollection` can be used as return values. If used without
AdvancedEnumeratorParam only `IEnumerable`/`ICollection`/`IReadOnlyCollection` could be used and it is ascending
order only.)

```C#
    public class Room
    {
        [PrimaryKey(1)]
        public ulong CompanyId { get; set; }
        [PrimaryKey(2)]
        public ulong Id { get; set; }
        public string Name { get; set; }
    }

    public interface IRoomTable
    {
        void Insert(Room room);
        IOrderedDictionaryEnumerator<ulong, Room> ListById(AdvancedEnumeratorParam<ulong> param);
        IOrderedDictionaryEnumerator<ulong, Room> ListById(ulong companyId, AdvancedEnumeratorParam<ulong> param);
    }
```

List also support variants with subset resulting types like `Find`.

### Scan

```C#
    IEnumerable<Room> ScanById(Constraint<ulong> companyId, Constraint<ulong> id);
```

Returns rows in ascending order of primary index matching various constraints which you can define at query time like
this:

```C#
    var oddRooms = table.ScanById(Constraint.Unsigned.Any, Constraint.Unsigned.Predicate(id => id % 2 == 1));
```

If you don't always need to constraint all fields it is better to add additional overloads with less constraints (
missing constraints are automatically "Any"):

    IEnumerable<Room> ScanById(Constraint<ulong> companyId);

    var roomsOf1 = table.ScanById(Constraint.Unsigned.Exact(1));

Scan by primary key also support variants like `ScanByIdVariantName`. Return type can be only `IEnumerable<T>`.

`Scan` can do most of the same stuff as `List`, it is a little bit slower though, so prefer `List` if you can. `Scan`
needs
to iterate all rows in many cases, because BTDB has all indexes in memory it is not slow even for millions of items,
still be careful.

### Gather

    ulong GatherById(ICollection<Room> target, long skip, long take, Constraint<ulong> companyId, Constraint<ulong> id);

Gather is like Scan with Count and Skip and Take. It is perfect to implement paging, when you need to calculate total
number of matching rows, but also return only rows from some position (skip) and at most some count (take). First
parameter can be anything inheriting from `ICollection<T>` only method which Gather calls from this interface is `Add`.
It means `target` does not need to be empty, it will just add new rows. Variants does not need to append VariantName to
method name, because it is defined by first parameter which you can easily overload.

### Gather with sorting/ordering

    ulong GatherById(ICollection<Room> target, long skip, long take, Constraint<ulong> companyId, Constraint<ulong> id, IOrderer[]? orderers);

All same like simple `Gather` but additionally as last parameter you can pass array of "orderers". You can order by
property included in used index. Sort is also stable, that means empty or null orderers will just do simple Gather
without sorting. Logical order of operations is where constraints, sort, skip, take.

```C#
    var target = new List<Room>();
    var count = table.GatherById(target, 0, 100, Constraint.Unsigned.Any, Constraint.Unsigned.Any, new [] {
        Orderer.Descending((Room v)=>v.Id),
        Orderer.Ascending((Room v)=>v.CompanyId) // This orderer is superfluous and it is better to not have it there because sorting will use less memory
        });
```

### FirstById

    Room FirstById(Constraint<ulong> companyId, Constraint<ulong> id);
    Room? FirstByIdOrDefault(Constraint<ulong> companyId, Constraint<ulong> id);
    Room FirstById(Constraint<ulong> companyId, Constraint<ulong> id, IOrderers[]? orderers);
    Room? FirstByIdOrDefault(Constraint<ulong> companyId, Constraint<ulong> id, IOrderers[]? orderers);

It is like GatherBy only with take one. It is faster because of that does not need to sort and allocate too much.
Version without OrDefault throws is not item matches.
First by primary key also support variants like `FirstByIdVariantName` and `FirstByIdOrDefaultVariantName`.

### LastById

    Room LastById(Constraint<ulong> companyId, Constraint<ulong> id);
    Room? LastByIdOrDefault(Constraint<ulong> companyId, Constraint<ulong> id);
    Room LastById(Constraint<ulong> companyId, Constraint<ulong> id, IOrderers[]? orderers);
    Room? LastByIdOrDefault(Constraint<ulong> companyId, Constraint<ulong> id, IOrderers[]? orderers);

It is like FirstById only it searches from the end.
Version without OrDefault throws is not item matches.
Last by primary key also support variants like `LastByIdVariantName` and `LastByIdOrDefaultVariantName`.

### Count

    uint|int|ulong|long CountById(AdvancedEnumeratorParam<ulong> param);
    uint|int|ulong|long CountById(ulong id);

`Count` is like `List` just returns total count of items and much faster.

### Any

    bool AnyById(AdvancedEnumeratorParam<ulong> param);
    bool AnyById(ulong id);

`Any` is like `List` just returns true when any item present and is much faster.

### Enumerate

    IEnumerator<Person> GetEnumerator();

Enumerates all items sorted by primary key. Must be Disposed.

### IReadOnlyCollection

All relations implements `IReadOnlyCollection<T>`. This can be used during debugging immediately, or directly in code -
after casting or defining like this: `public interface IRoomTable : IReadOnlyCollection<Room>`.

### Query

```C#
    var adults = table.Query<Person>()
        .Where(p => p.TenantId == tenantId && p.Age >= 18 && p.Name != null && p.Name.StartsWith("A"))
        .ToList();
```

`Query<T>()` returns an `IQueryable<T>`. `T` is the item type or a registered projection of it. `Where` predicates are
compiled to interpreter programs and tested directly on the stored bytes. Only matching rows are materialized, so
selective queries are faster than enumerating the relation and filtering with LINQ. Everything works without
`Reflection.Emit`, so it is usable with NativeAOT.

Rows are returned in primary key order. Conditions on key fields decide which part of the relation is scanned:

- Equalities on leading primary key fields become a key prefix.
- `Contains` on a constant collection, or `||` of equalities on such a field, becomes one seek per value, merged in key
  order. At most 1024 seeks are used; larger lists are tested per row instead.
- After those, one more field can limit a range:
  - `<`, `<=`, `>`, `>=` on integer, `bool` and enum fields;
  - ordinal `StartsWith` on a string field.
- A secondary index is used when its fields allow a longer prefix or range than the primary key; ties go to the
  primary key.
  - Index keys are tested first by conditions on index fields.
  - Primary rows are read only for the remaining index keys, and are still returned in primary key order.
  - `Count`, `Any`, `All`, `Min` and `Max` read only the index when it has every field they need.

Equality and range conditions are only used this way when every condition written before them cannot throw, so the
result stays exactly as with LINQ.

Supported in `Where`:

- **Logic:** `&&`, `||`, `!`, and `bool` properties used directly, such as `p.Active` or `!p.Active`.
- **Comparisons:** `==`, `!=`, `<`, `<=`, `>`, `>=`.
  - A property can be compared with a constant, a captured variable or another property.
  - Arithmetic is allowed in comparisons: `+`, `-`, `*`, `/`, `%`, unary minus, and the `checked` variants.
  - So are numeric casts, enum-to-number casts and casts to or from nullable types.
- **Field types:** `string`, `bool`, all integer types, `Half`, `float`, `double`, `decimal`, `DateTime`,
  `DateTimeOffset`, `TimeSpan`, `Guid` and enums.
- **Nullable values:** `== null`, `HasValue`, `.Value`, and lifted comparisons and arithmetic with C# semantics.
- **Strings:**
  - `==`, `!=` and `string.Equals`, with an optional `StringComparison`;
  - `StartsWith`, `EndsWith` and `Contains`, with a constant `string` or `char` argument and an optional
    `StringComparison`;
  - `Length`, `string.IsNullOrEmpty`, and `string.CompareOrdinal(...)` compared to `0`.
  - String values are compared without allocating. Equality and ordinal `StartsWith` compare encoded bytes and do not
    decode the string at all.
- **`Contains` on constant collections:** arrays, `List<T>` and `HashSet<T>` with the default comparer, for example
  `ids.Contains(p.Id)`.

Predicates keep C# semantics, including exceptions:
- Calling a string method on a null string throws `NullReferenceException`.
- `.Value` of a null nullable throws `InvalidOperationException`.
- Integer division by zero throws `DivideByZeroException`.

Conditions are not always evaluated in the written order: cheaper and key-only conditions may run first. Even so, a
condition that can throw only runs after every condition written before it, so guards like
`p.Name != null && p.Name.StartsWith("A")` work as in LINQ.

`OrderBy`, `OrderByDescending`, `ThenBy`, `ThenByDescending`, `Skip` and `Take` follow `Where`:
- **Ordering by key fields** in primary key order, ignoring fields fixed by equalities, streams rows without sorting,
  for example `Where(p => p.TenantId == t).OrderBy(p => p.Id)`. The same holds descending, when the ordering covers
  all remaining key fields.
- **Other orderings** sort the matching rows in memory by keys read from the stored bytes. The sort is stable, and
  strings compare by the current culture like `Comparer<string>.Default`.
  - With `Take`, only `Skip + Take` rows are kept.
  - Only the returned rows are materialized.
- **`Skip`/`Take` without ordering** stop the scan at the end of the page.
- **Without a filter,** `Skip` jumps by key indexes in O(log n).
- **Exceptions:** like LINQ, an ordered query tests every row before returning the first one, so ordering streams only
  when no condition can throw. `Take(0)` tests no row.
- **Terminal operators,** including aggregates, work on ordered and paged sources. Sums and averages follow the page
  order like LINQ.
`Select` follows `Where` and ordering, for example
`table.Query<Person>().Where(p => p.Age > 18).Select(p => new { p.Name, p.Age })`:
- It supports single values (properties or expressions over them), anonymous types, constructors, `ValueTuple` and
  `Tuple`, records, and member initializers. `[Generate]` classes are filled through their generated setters.
- Only the fields the projection needs are read from the stored bytes, and only for returned rows. Like LINQ, the
  selector runs only when an element is produced: `Count` runs it on every row, `Last` only on the last one.
- `Skip`, `Take` and terminal operators follow it. `Min`, `Max`, `Sum` and `Average` without a selector aggregate the
  projected value.
- `Where`, ordering and a terminal predicate after `Select` are not supported, nor is the indexed
  `Select((p, i) => ...)`.

- **Not supported:**
  - `Where` after `OrderBy`, `Skip` or `Take`;
  - ordering keys that can throw;
  - a predicate argument of a terminal operator after `Skip` or `Take`, or after `OrderBy` when the predicate can
    throw.

Terminal operators run on the stored rows too, with their predicate overloads:
- `Count` and `LongCount` never materialize rows. When the conditions are fully covered by key prefixes and seeks,
  of the primary key or of a secondary index, they only compute key indexes in O(log n).
- `Any` and `All` stop at the first deciding row.
- `First`, `FirstOrDefault`, `Single`, `SingleOrDefault`, `Last` and `LastOrDefault` materialize only the returned
  row. The `OrDefault` overloads with a default value are supported.
- `Min`, `Max`, `Sum` and `Average` take a selector, for example `table.Query<Person>().Sum(p => p.Age * 2)`. They
  read only the fields the predicate and the selector need.

Results, exceptions and which rows get tested are the same as LINQ-to-Objects over the materialized list:
- checked `Sum` of integers;
- floats summed as `double`;
- null values skipped;
- `InvalidOperationException` for an empty non-nullable `Min`, `Max` or `Average`;
- `Last` searching backwards from the end, except `Last(predicate)` after `Where`, which tests every row.

Anything else throws `NotSupportedException` when the query is enumerated or executed. Examples:
- other LINQ operators, such as `SelectMany`, `Distinct` or `TakeLast`;
- `Min`, `Max`, `Sum` and `Average` without a selector or over strings;
- comparing two string properties;
- string concatenation;
- `string.Compare` with a culture.

Use `AsEnumerable()` to continue in memory.

Compiled queries are cached per relation by the shape of their expression, so running the same query again, also in
another transaction or thread, skips conversion and compilation. Captured variables, and values like `Skip` and
`Take` counts, are parameters of the cached plan:
- **Bound for each execution:** values compared, computed or selected, key equalities and bounds, and paging.
- **Cached per value:** other uses, for example a string compared to a property without an index.
- **Never cached:** captured collections, for example for `Contains`, because their contents can change.

The cache keeps at most 256 query shapes per relation.

With NativeAOT, keep the operators of `decimal` (for example
`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicMethods, typeof(decimal))]`) when predicates compare or
compute with `decimal`. The C# compiler builds those expression trees without operator metadata, and
System.Linq.Expressions looks the operators up by reflection.

## Primary Key

One or more fields can be selected as primary key. Primary key must be unique in the relation. Order of fields in
primary key is marked as parameter of `PrimaryKey(i)` attribute. Methods expecting primary key as an argument are
supposed to contain all fields in the same order as defined, for example in this case:

```C#
    public class Person
    {
        [PrimaryKey(1)]
        public ulong TenantId { get; set; }
        [PrimaryKey(2)]
        public ulong Id { get; set; }
        ...
    }
```

methods will look like:

```C#
    bool RemoveById(ulong tenantId, ulong id);
    Person FindById(ulong tenantId, ulong id);
```

## Secondary Key

Secondary keys are useful for fast access by other fields then primary key. Declared are as attribute `SecondaryKey`.
Each secondary index has it's name (could be different from existing fields names). Secondary index may be compound from
several fields. Each field can be part of more than one secondary key. for example:

```C#
    public class Person
    {
        [PrimaryKey(1)]
        public ulong TenantId { get; set; }
        [PrimaryKey(2)]
        public ulong Id { get; set; }
        [SecondaryKey("Age", Order = 2)]
        [SecondaryKey("Name", IncludePrimaryKeyOrder = 1)]
        public string Name { get; set; }
        [SecondaryKey("Age", IncludePrimaryKeyOrder = 1)]
        public uint Age { get; set; }
    }
```

we have two indexes Age and Name. They are serialized in form:

    "Age": TenantId, Age, Name, Id => void
    "Name": TenantId, Name, Id => void

It is always possible to insert duplicate items for secondary key (it would cause problems when adding new indexes
during upgrade). That's why secondary field contains in key also all primary key fields which ensures they are unique.
From this key is always possible to construct primary key. `IncludePrimaryKeyOrder` can propagate up the primary keys -
typically useful for keeping together data for one tenant (kind of partitioning).

### List (by secondary index)

    IEnumerable<Person> ListByAge(AdvancedEnumeratorParam<uint> param);

List by ascending/descending order and specified range, see `CanIterateBySecondaryKey`
in [ObjectDbTableTest](../BTDBTest/ObjectDbTableTest.cs)
`ListBy{SecondaryIndexName}([secKeyField(1),... secKeyField(N-1),] AdvancedEnumeratorParam<typeof(secKeyField(N))>)`

List by secondary key also support variants like `ListByAgeVariantName`.

### Count (by secondary index)

    uint|int|long|ulong CountByAge(AdvancedEnumeratorParam<uint> param);
    uint|int|long|ulong CountByAge(uint age);

Count records by specified
range
`CountBy{SecondaryIndexName}([secKeyField(1),... secKeyField(N-1),] [AdvancedEnumeratorParam<typeof(secKeyField(N))>)]`

### Any (by secondary index)

    bool AnyByAge(AdvancedEnumeratorParam<uint> param);
    bool AnyByAge(uint age);

Returns true if there is any item in specified
range
`AnyBy{SecondaryIndexName}([secKeyField(1),... secKeyField(N-1),] [AdvancedEnumeratorParam<typeof(secKeyField(N))>)]`

### Scan (by secondary index)

    IEnumerable<Person> ScanByAge(Constraint<ulong> tenantId, Constraint<ulong> age, Constraint<string> name, Constraint<ulong> id);
    IEnumerable<Person> ScanByName(Constraint<ulong> tenantId, Constraint<string> name);

Returns rows in ascending order of secondary index matching various constraints which you can define at query time like
this:

    var ageOver30 = table.ScanByAge(Constraint.Unsigned.Any, Constraint.Unsigned.Predicate(age => age > 30), Constraint.String.Any, Constraint.Unsigned.Any);
    var namesFromTentant1StartingWithBob = table.ScanByName(Constraint.Unsigned.Exact(1), Constraint.String.StartsWith("Bob"));

ScanByAge example also shows you can add constrains to primary key fields not mentioned in secondary key. Parameter
order must match serialization order and parameter names must match field names case insensitively.

If you don't always need to constraint all fields it is better to add additional overloads with less constraints (
missing constraints are automatically "Any"):

    IEnumerable<Person> ScanByAge(Constraint<ulong> tenantId, Constraint<ulong> age);

    var age45 = table.ScanByAge(Constraint.Unsigned.Any, Constraint.Unsigned.Exact(45));

Scan by secondary key also support variants like `ScanByAgeVariantName`.

### Gather (by secondary index)

    ulong GatherByName(List<Person> toFill, long skip, long take, Constraint<ulong> tenantId, Constraint<string> name);

It is exactly same counterpart for Scan like in primary key case. Also could be used with orderers.

### First (by secondary index)

    Person FirstByName(Constraint<ulong> tenantId, Constraint<string> name);
    Person? FirstByNameOrDefault(Constraint<ulong> tenantId, Constraint<string> name);
    Person FirstByName(Constraint<ulong> tenantId, Constraint<string> name, IOrderers[]? orderers);
    Person? FirstByNameOrDefault(Constraint<ulong> tenantId, Constraint<string> name, IOrderers[]? orderers);

It is like GatherBy only with take one. It is faster because of that does not need to sort and allocate too much.
Version without OrDefault throws is not item matches.
First by secondary key also support variants like `FirstByNameVariantName` and `FirstByNameOrDefaultVariantName`.

### Last (by secondary index)

    Person LastByName(Constraint<ulong> tenantId, Constraint<string> name);
    Person? LastByNameOrDefault(Constraint<ulong> tenantId, Constraint<string> name);
    Person LastByName(Constraint<ulong> tenantId, Constraint<string> name, IOrderers[]? orderers);
    Person? LastByNameOrDefault(Constraint<ulong> tenantId, Constraint<string> name, IOrderers[]? orderers);

It is like FirstByName only it searches from the end.
Version without OrDefault throws is not item matches.
Last by secondary key also support variants like `LastByNameVariantName` and `LastByNameOrDefaultVariantName`.


### Upgrade

When secondary definition is changed (for example new index is defined) then it is automatically
added/recalculated/removed in `InitRelation` call. You can see examples in
[ObjectDbTableUpgradeTest](../BTDBTest/ObjectDbTableUpgradeTest.cs)

## Free content

During removing or updating of data, all IDictionaries, IOrderedSets, IRoaringBitmaps, and ILazyUlongLists present in
removed data are
automatically cleared to avoid data leaks (Also works recursively IDictionaries are freed automatically if they are
nested in another IDictionary). You can see examples in
[ObjectDbTableFreeContentTest](../BTDBTest/ObjectDbTableFreeContentTest.cs)

When a lazy `IDictionary<TKey, TValue>` has a class as `TValue`, direct dictionary mutations normally free nested content
inside removed or replaced values. A nested lazy dictionary can be copied to another object and then share the same
dictionary ID. Applications using this pattern can open ObjectDB with
`new DBOptions().WithoutFreeContentInNativeObject()` to disable value free-content handling for direct lazy-dictionary
mutations. Normal relation free-content cleanup stays enabled. Unreferenced content remains leaked until the compactor
leak detector runs with `CompactorLeakDetectorMode.Erase`.

If you have IIndirect property. You are on your own. And that's include any nested IDictionary which needs to be cleared
before. So you need recursively load objects and delete them. See test named `IIndirectMustBeFreedManually`
in [ObjectDbTableFreeContentTest](../BTDBTest/ObjectDbTableFreeContentTest.cs).

## Modification check during enumeration

When you Insert, RemoveById or insert item using Upsert during enumerating relation an exception will be thrown. It is
still possible to modify by Update (or Upsert for existing items) see `CheckModificationDuringEnumerate`
in [ObjectDbTableTest](../BTDBTest/ObjectDbTableTest.cs) for details. Modification of secondary indexes during
enumerating by secondary indexes are not detected at this moment.

## OnCreate

When relation is created for first time, BTDB will try to resolve `IRelationOnCreate<InterfaceOfRelation>`. It can be
used for upgrading data from old relations or what ever into new Relation. Very useful when you need to change primary
key and not loose data. Example:

    public class Table2OnCreate : IRelationOnCreate<ITableV2>
    {
        public void OnCreate(IObjectDBTransaction transaction, ITableV2 creating)
        {
            var from = transaction.GetRelation<ITableV1>();
            creating.UpsertRange(from.As<ItemV2>());
            from.RemoveAll();
        }
    }

    var builder = new ContainerBuilder();
    builder.RegisterType<Table2OnCreate>().As<IRelationOnCreate<ITableV2>>();
    var container = builder.Build();

    db.Open(kvdb, false, new DBOptions().WithContainer(container));

## OnSerialize

You can define any number of parameter less void returning methods which will be run before any inserting or updating
object to relation.

```C#
    public class Room
    {
        [PrimaryKey(1)]
        public ulong CompanyId { get; set; }
        public string Name { get; set; }

        [OnSerialize]
        void AnyMethodName()
        {
            Name ??= "Unknown";
        }
    }
```

## OnBeforeRemove

You can define any number of instance methods with OnBeforeRemove attribute.
This is useful for cases you have foreign key to this relation and you need to check if it is safe to remove this object
or you can implement cascade delete.
Only result types allowed are void and bool. If bool is returned then true means that remove is prevented. If there are
multiple methods then at least one of them must return true to prevent remove, but all methods are called in all cases
and you should not dependent on their order of calling.
As parameters you can use IObjectDBTransaction. All other parameters must be resolvable by IContainer.
Of course nothing is free, if you define any OnBeforeRemove method then all removes especially range removes are slower
because they need to deserialize all removing items.

```C#
    public class Room
    {
        [PrimaryKey(1)]
        public ulong CompanyId { get; set; }
        public string Name { get; set; }

        [OnBeforeRemove]
        bool AnyMethodName(IObjectDBTransaction transaction, IContainer container)
        {
            return true; // true prevents remove
        }
    }
```

## Interface contracts for class relation items

For source-generated relations, `T` in `IRelation<T>` is still a class. The generator can use interfaces implemented by
that class (including inherited interfaces) as the source of persisted fields/indexes and lifecycle hooks.
`OnSerialize` and `OnBeforeRemove` methods can be declared on the interface contract.

```C#
    public interface IRoom
    {
        [PrimaryKey(1)]
        ulong CompanyId { get; set; }

        string Name { get; set; }

        [OnSerialize]
        void Normalize();

        [OnBeforeRemove]
        bool CanRemove(IObjectDBTransaction transaction);
    }

    public partial class Room : IRoom
    {
    }

    public partial class Room
    {
        public virtual ulong CompanyId { get; set; }
        public virtual string Name { get; set; } = "";

        public virtual void Normalize()
        {
        }

        public virtual bool CanRemove(IObjectDBTransaction transaction)
        {
            return false;
        }
    }

    public interface IRoomTable : IRelation<Room>
    {
        Room FindById(ulong companyId);
    }
```

This is useful when the class is generated in multiple partial declarations and one generator provides the interface
contract while another provides implementations.

## ShallowUpsertWithSizes

```C#
(bool Inserted, uint KeySize, uint OldValueSize, uint NewValueSize) ShallowUpsertWithSizes(T obj, bool allowInsert, bool allowUpdate)
```

Special method which allow to get used sizes for key and values and supporting also only insert (obj, true, false) and
only update (obj, false, true) behaviors.
If called with (obj, false, false) it could be used for getting if obj exists in relation (KeySize>0) and stored value
length (OldValueSize).

Currently, limitation is that it could be used only for relations without secondary keys.

Note: It is "shallow" version only, so don't use with properties of IDictionary type, or it could leak data.

## RemoveWithSizesById

```C#
(ulong Count, ulong KeySizes, ulong ValueSizes) RemoveWithSizesById(Constraint<ulong> tenantId, Constraint<string> key);
```

Allow very quickly remove rows if you complain to limitations.

First limitation that it could be used only for constraints which are implementable by key prefix search.
Second limitation is that it could be used only for relations without secondary keys.
Also, it does not FreeContent so don't use with properties of IDictionary type (it will throw).

## InKeyValue

```C#
    public class Person
    {
        [PrimaryKey(1)]
        public uint TenantId { get; set; }

        [PrimaryKey(2)]
        public string? Email { get; set; }

        // Next line could be written as [PrimaryKey(3, InKeyValue = true)]
        // but it is more idiomatic to use InKeyValue attribute
        [InKeyValue(3)]
        public DateTime LastLogin { get; set; }

        public string? Name { get; set; }
    }

    public interface IPersonTable : IRelation<Person>
    {
        IEnumerable<Person> ScanById(Constraint<ulong> tenantId, Constraint<string> email, Constraint<DateTime> lastLogin);
        bool UpdateById(uint tenantId, string email, DateTime lastLogin);
    }
```

Fields with InKeyValue are allowed only at end of primary key, and they cannot be in any secondary keys.
Primary key without InKeyValue fields must be unique. In example above it means that where cannot be two persons with
same tenant and email with just different LastLogins. These InKeyValue fields could be used in Scan/GatherById for
relatively fast searching without need to introduce Secondary Keys. This is very useful for implementation of expiration
of relation rows. UpdateById method in example could be used for modification of LastLogin for known tenantId and email.

Data in database will be automatically upgraded when only InKeyValue fields are added/removed/modified in primary key
definition.

## Computed fields

```C#
    public class Person
    {
        [PrimaryKey(1)]
        public uint Id { get; set; }

        [PrimaryKey(2)]
        public string? Email { get; set; }

        public string? Name { get; set; }

        [SecondaryKey("LowerCasedName", IncludePrimaryKeyOrder = 1)]
        public string LowerCasedName => Name?.ToLowerInvariant() ?? "";
    }

    public interface IPersonTable : IRelation<Person>
    {
        Person FindByLowerCasedName(uint id, string lowerCasedName);
    }
```

Properties with only getter and no setter are considered computed fields.
In example above `LowerCasedName` is computed field.
They are not stored in database value, just in secondary indexes.
They are automatically recalculated when needed, it makes updating relation little bit slower because
old value must be deserialized into object to calculate old value of secondary index. But it is faster than storing that
string twice in value.
It is not possible to use computed fields in primary key.

### Cursor access during fast iteration

`IKeyValueDBCursor.FastIterate` maintains the current position during each callback, allowing key/value reads.
The callback must not move or invalidate the cursor, reenter iteration, or modify the database.

`FastIterateNoCursor` is a separate API for callbacks that consume only the supplied key index and key span.
Its callback must not access the iterating cursor or modify the database. The key span is valid only during
that callback. Cursor state is published when iteration returns or throws; an early stop or exception leaves
it on the current key. The lower-level `ICursor` overload also requires that the callback not access its
`ref keyIndex` argument. Primary-key-only `IterateById` uses this API internally with its private cursor.

Automatic relation registration from a read-only transaction stays in memory without opening a hidden writer.
The first writing transaction that accesses the relation persists its schema, applies pending index upgrades,
and runs its creation callback. An unrelated writing transaction does not initialize the relation. Rollback leaves
initialization pending for the next writer. Reads before an index upgrade use the existing stored indexes; call
`InitializeRelations` before application access when all indexes must be ready at startup.
