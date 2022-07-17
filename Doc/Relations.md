# Relations

Relations provides easy way how to store "table" like data in object db.

Let's first define data entity we want to store (note that it is not defined as `[StoredInline]`, but it is still stored inline). Such objects also don't have object id, they can be retrieved by primary or secondary indexes.

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

From transaction we get creator of relation which we should keep and use for creating relation interface for transaction every time we need it.

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

It is still good to do first GetRelation for all your relations in first independent transaction. To control name of relation by `PersistedNameAttribute` on your `IRelation` interface.

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

Faster update but can change only nonprimary fields and must be simple types (no classes, IIndirect<T>, IDictionary<K,V>, IOrderedSet<T>). Value properties could be any number and in any order by they must match parameter name case insensitively. Returns true if found and updated, void variant throw when does not exists. Any suffix could be appended to function name `UpdateById`, use to disambiguate when multiple overrides would have same types of value sets.

### Upsert (Insert or Update)

    var inserted = personTable.Upsert(new Person { Id = 2, Name = "superadmin", Age = 100 });

returns true if inserted, false if updated

Note: `Upsert` is automatically always available if you inherit from `IRelation<T>`

### ShallowUpdate

It is like `Update`, but it does not try to compare and free any nested content. It is especially faster without secondary indexes when it does not even need to read old value.

### ShallowUpsert

It is like `Upsert`, but it does not try to compare and free any nested content. It is especially faster without secondary indexes when it does not even need to read old value.

### Remove

    (void|bool) RemoveById(primaryKey1, ..., primaryKeyN);
    (void|bool) ShallowRemoveById(primaryKey1, ..., primaryKeyN);

Returns true if removed, void variant throw when does not exists. All primary keys fields are used as parameters, for example `void RemoveById(ulong tenantId, ulong userId);`
ShallowRemoveById does not free nested content (like `IDictionary<K,V>`).

    int RemoveById(primaryKey1 [, primaryKey2, ...]);

Returns number of records removed for given primary key prefix for example `int RemoveById(ulong tenantId)` removes all users for given tenant

    int RemoveByIdPartial(primaryKey1 [, primaryKey2, ...] , maxCount);

additionally can be limited number of deleted items at once

```C#
    int RemoveByIdPartial(ulong tenantId, int maxCount)
```

advanced enumeration param can be used same way as in ListById

```C#
    int RemoveById(primaryKey1, primaryKey2, ..., primaryKey_N-1, AdvancedEnumeratorParam<typeof(primaryKeyField(N))>);
```

### Contains

    bool Contains(primaryKey1, ..., primaryKeyN);

Returns true if exist item with given primary key. All primary keys fields are used as parameters, for example `bool Contains(ulong tenantId, ulong userId);`

### Find

    Person FindById(ulong id);

It will throw if does not exists, as parameters expects primary key fields (same as in RemoveById)

    Person FindByIdOrDefault(ulong id);

Will return null if not exists

    IEnumerator<T> FindById(primaryKey1 [, primaryKey2, ...]);

Find all items with given primary key prefix

    Person FindByAgeOrDefault(uint age);

Find by secondary key, it will throw if it find multiple Persons with that age. **Note**: "Age" in the name is name of secondary key index.

    IEnumerator<Person> FindByAge(uint age);

Find all items with given secondary key. **Note**: for advanced range enumerating use ListBy{SecondaryIndexName}, multiple result possibility handles legal case when exists several records for one secondary index key.

Find support returning also not item type but any subset type, but because you cannot have same name of method which differs only by return type you can append any text to make it unique. This is useful for speed up deserialization because only fields with matching names and types will be deserialized. Note: This feature is also called Variants.

    public class Age
    {
        public uint Age { get; set; }
    }

    IEnumerator<Age> FindByIdJustAge(ulong id);

### List

    IOrderedDictionaryEnumerator<uint, Person> ListById(AdvancedEnumeratorParam<uint> param);
    IEnumerator<Person> ListById(AdvancedEnumeratorParam<uint> param);
    IEnumerable<Person> ListById();

List by ascending/descending order and specified range. Parts of primary key may be used for listing. In example below you can list all rooms or just rooms for specified company by two `ListById` method. (`IOrderedDictionaryEnumerator`, `IEnumerator`, `IEnumerable` can be used as return values if used without AdvancedEnumeratorParam only `IEnumerator` or `IEnumerable` could be used and it is ascending order only.)

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

Returns rows in ascending order of primary index matching various constraints which you can define at query time like this:

```C#
    var oddRooms = table.ScanById(Constraint.Unsigned.Any, Constraint.Unsigned.Predicate(id => id % 2 == 1));
```

If you don't always need to constraint all fields it is better to add additional overloads with less constraints (missing constraints are automatically "Any"):

    IEnumerable<Room> ScanById(Constraint<ulong> companyId);

    var roomsOf1 = table.ScanById(Constraint.Unsigned.Exact(1));

Scan by primary key also support variants like `ScanByIdVariantName`.

`Scan` can do most of same stuff like `List`, it is little bit slower though, so prefer `List` if you can. `Scan` needs to iterate all rows in many cases, because BTDB has all indexes in memory it is not slow even for millions of items, still be careful.

### Gather

    ulong GatherById(ICollection<Room> target, long skip, long take, Constraint<ulong> companyId, Constraint<ulong> id);

Gather is like Scan with Count and Skip and Take. It is perfect to implement paging, when you need to calculate total number of matching rows, but also return only rows from some position (skip) and at most some count (take). First parameter can be anything inheriting from `ICollection<T>` only method which Gather calls from this interface is `Add`. It means `target` does not need to be empty, it will just add new rows. Variants does not need to append VariantName to method name, because it is defined by first parameter which you can easily overload.

### Garter with sorting/ordering

    ulong GatherById(ICollection<Room> target, long skip, long take, Constraint<ulong> companyId, Constraint<ulong> id, IOrderers[]? orderers);

All same like simple `Gather` but additionally as last parameter you can pass array of "orderers". You can order by property included in used index. Sort is also stable, that means empty or null orderers will just do simple Gather without sorting. Logical order of operations is where constraints, sort, skip, take.

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

It is like GatherBy only with take one. It is faster because of that does not need to sort and allocate too much. Version without OrDefault throws is not item matches.
First by primary key also support variants like `FirstByIdVariantName` and `FirstByIdOrDefaultVariantName`.

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

Enumerates all items sorted by primary key.

### IReadOnlyCollection

All relations implements `IReadOnlyCollection<T>`. This can be used during debugging immediately, or directly in code - after casting or defining like this: `public interface IRoomTable : IReadOnlyCollection<Room>`.

## Primary Key

One or more fields can be selected as primary key. Primary key must be unique in the relation. Order of fields in primary key is marked as parameter of `PrimaryKey(i)` attribute. Methods expecting primary key as an argument are supposed to contain all fields in the same order as defined, for example in this case:

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

Secondary keys are useful for fast access by other fields then primary key. Declared are as attribute `SecondaryKey`. Each secondary index has it's name (may be different then existing fields names). Secondary index may be compound from several fields. Each field can be part of more than one secondary key. for example:

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

It is always possible to insert duplicate items for secondary key (it would cause problems when adding new indexes during upgrade). That's why secondary field contains in key also all primary key fields which ensures they are unique. From this key is always possible to construct primary key. `IncludePrimaryKeyOrder` can propagate up the primary keys - typically useful for keeping together data for one tenant (kind of partitioning).

### List (by secondary index)

    IEnumerable<Person> ListByAge(AdvancedEnumeratorParam<uint> param);

List by ascending/descending order and specified range, see `CanIterateBySecondaryKey` in [ObjectDbTableTest](../BTDBTest/ObjectDbTableTest.cs)
`ListBy{SecondaryIndexName}([secKeyField(1),... secKeyField(N-1),] AdvancedEnumeratorParam<typeof(secKeyField(N))>)`

List by secondary key also support variants like `ListByAgeVariantName`.

### Count (by secondary index)

    uint|int|long|ulong CountByAge(AdvancedEnumeratorParam<uint> param);
    uint|int|long|ulong CountByAge(uint age);

Count records by specified range `CountBy{SecondaryIndexName}([secKeyField(1),... secKeyField(N-1),] [AdvancedEnumeratorParam<typeof(secKeyField(N))>)]`

### Any (by secondary index)

    bool AnyByAge(AdvancedEnumeratorParam<uint> param);
    bool AnyByAge(uint age);

Returns true if there is any item in specified range `AnyBy{SecondaryIndexName}([secKeyField(1),... secKeyField(N-1),] [AdvancedEnumeratorParam<typeof(secKeyField(N))>)]`

### Scan (by secondary index)

    IEnumerable<Person> ScanByAge(Constraint<ulong> tenantId, Constraint<ulong> age, Constraint<string> name, Constraint<ulong> id);
    IEnumerable<Person> ScanByName(Constraint<ulong> tenantId, Constraint<string> name);

Returns rows in ascending order of secondary index matching various constraints which you can define at query time like this:

    var ageOver30 = table.ScanByAge(Constraint.Unsigned.Any, Constraint.Unsigned.Predicate(age => age > 30), Constraint.String.Any, Constraint.Unsigned.Any);
    var namesFromTentant1StartingWithBob = table.ScanByName(Constraint.Unsigned.Exact(1), Constraint.String.StartsWith("Bob"));

ScanByAge example also shows you can add constrains to primary key fields not mentioned in secondary key. Parameter order must match serialization order and parameter names must match field names case insensitively.

If you don't always need to constraint all fields it is better to add additional overloads with less constraints (missing constraints are automatically "Any"):

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

It is like GatherBy only with take one. It is faster because of that does not need to sort and allocate too much. Version without OrDefault throws is not item matches.
First by secondary key also support variants like `FirstByNameVariantName` and `FirstByNameOrDefaultVariantName`.

### Upgrade

When secondary definition is changed (for example new index is defined) then it is automatically added/recalculated/removed in `InitRelation` call. You can see examples in
[ObjectDbTableUpgradeTest](../BTDBTest/ObjectDbTableUpgradeTest.cs)

## Free content

During removing or updating of data, all IDictionaries and IOrderedSets present in removed data are automatically cleared to avoid data leaks (Also works recursively IDictionaries are freed automatically if they are nested in another IDictionary). You can see examples in
[ObjectDbTableFreeContentTest](../BTDBTest/ObjectDbTableFreeContentTest.cs)

If you have IIndirect property. You are on your own. And that's include any nested IDictionary which needs to be cleared before. So you need recursively load objects and delete them. See test named `IIndirectMustBeFreedManually` in [ObjectDbTableFreeContentTest](../BTDBTest/ObjectDbTableFreeContentTest.cs).

## Modification check during enumeration

When you Insert, RemoveById or insert item using Upsert during enumerating relation an exception will be thrown. It is still possible to modify by Update (or Upsert for existing items) see `CheckModificationDuringEnumerate` in [ObjectDbTableTest](../BTDBTest/ObjectDbTableTest.cs) for details. Modification of secondary indexes during enumerating by secondary indexes are not detected in this moment.
