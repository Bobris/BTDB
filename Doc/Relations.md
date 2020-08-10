# Relations

Relations provides easy way how to store "table" like data in object db.

Let's first define data entity we want to store (note that it is not ne defined as [StoredInline] but it is still inlined)

    public class Person
    {
        [PrimaryKey(1)]
        public ulong Id { get; set; }
        public string Name { get; set; }
        [SecondaryKey("Age")]
        public ulong Age { get; set; }
    }

    public interface IPersonTable
    {
        void Insert(Person person);
        bool RemoveById(ulong id);
        Person FindById(ulong id);
    }

How do we get `IPersonTable` interface to actually insert persons? First we need [obtain transaction](ODBDictionary.md)

From transaction we get creator of relation which we should keep and use for creating relation interface for transaction every time we need it.

    Func<IObjectDBTransaction, IPersonTable> creator;
    using (var tr = _db.StartTransaction())
    {
        creator = tr.InitRelation<IPersonTable>("Person");
        var personTable = creator(tr);
        personTable.Insert(new Person { Id = 2, Name = "admin", Age = 100 });
        tr.Commit();
    }

next time we reuse creator:

    using (var tr = _db.StartTransaction())
    {
        var personTable = creator(tr);
        return personTable.FindById(100);
    }

`InitRelation` can be called only once.

## Basic operations

When defined in interface following methods are automatically implemented by BTDB when defined in relation interface

### Insert

    (void|bool) personTable.Insert(new Person { Id = 2, Name = "admin", Age = 100 });

**void** variant will throw if already exists

**bool** variant returns true when inserted, false when already exists (without changing value)

### Update

    personTable.Update(new Person { Id = 2, Name = "superadmin", Age = 100 });

will throw if does not exist

### Upsert (Insert or Update)

    var inserted = personTable.Upsert(new Person { Id = 2, Name = "superadmin", Age = 100 });

return true if inserted

### ShallowUpdate

It is like `Update`, but it does not try to compare and free any nested content. It is especially faster without secondary indexes when it does not even need to read old value.

### ShallowUpsert

It is like `Upsert`, but it does not try to compare and free any nested content. It is especially faster without secondary indexes when it does not even need to read old value.

### Remove

    (void|bool) RemoveById(primaryKey1, ..., primaryKeyN);
    (void|bool) ShallowRemoveById(primaryKey1, ..., primaryKeyN);

Returns true if removed, void variant throw when does not exists. All primary keys fields are used as parameters, for example `void RemoveById(ulong tenantId, ulong userId);`
ShallowRemoveById does not free content.

    int RemoveById(primaryKey1 [, primaryKey2, ...]);

Returns number of records removed for given primary key prefix for example `int RemoveById(ulong tenantId)` removes all users for given tenant

    int RemoveByIdPartial(primaryKey1 [, primaryKey2, ...] , maxCount);

additionally can be limited number of deleted items at once

    `int RemoveByIdPartial(ulong tenantId, int maxCount)`

advanced enumeration param can be used same way as in ListById

    `int RemoveById(primaryKey1 , primaryKey2, ..., primaryKey_N-1, AdvancedEnumeratorParam<typeof(primaryKeyField(N))>);`

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

Find support returning also not item type but any subset type, but because you cannot have same name of method which differs only by return type you can append any text to make it unique. This is useful for speed up deserialization because only fields with matching names and types will be deserialized.

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

List also support variants with subset resulting types like `Find`.

### Count

    uint|int|ulong|long CountById(AdvancedEnumeratorParam<ulong> param);
    uint|int|ulong|long CountById(ulong id);

`Count` is like `List` just returns total count of items and much faster.

### Enumerate

    IEnumerator<Person> GetEnumerator();

Enumerates all items sorted by primary key.

### IReadOnlyCollection

All relations implements `IReadOnlyCollection<T>`. This can be used during debugging immediately, or directly in code - after casting or defining like this: `public interface IRoomTable : IReadOnlyCollection<Room>`.

## Primary Key

One or more fields can be selected as primary key. Primary key must be unique in the relation. Order of fields in primary key is marked as parameter of `PrimaryKey(i)` attribute. Methods expecting primary key as an argument are supposed to contain all fields in the same order as defined, for example in this case:

    public class Person
    {
        [PrimaryKey(1)]
        public ulong TenantId { get; set; }
        [PrimaryKey(2)]
        public ulong Id { get; set; }
        ...
    }

will methods look like:

    bool RemoveById(ulong tenantId, ulong id);
    Person FindById(ulong tenantId, ulong id);

## Secondary Key

Secondary keys are useful for fast access by other fields then primary key. Declared are as attribute `SecondaryKey`. Each secondary index has it's name (may be different then existing fields names). Secondary index may be compound from several fields. Each field can be part of more than one secondary key. for example:

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

we have two indexes Age and Name. They are serialized in form:

    "Age": TenantId, Age, Name, Id => void
    "Name": TenantId, Name, Id => void

It is always possible to insert duplicate items for secondary key (it would cause problems when adding new indexes during upgrade). That's why secondary field contains in key also all primary key fields which ensures they are unique. From this key is always possible to construct primary key. `IncludePrimaryKeyOrder` can propagate up the primary keys - typically useful for keeping together data for one tenant.

### List (by secondary index)

    IEnumerator<Person> ListByAge(AdvancedEnumeratorParam<uint> param);

List by ascending/descending order and specified range, see `CanIterateBySecondaryKey` in [ObjectDbTableTest](../BTDBTest/ObjectDbTableTest.cs)
`ListBy{SecondaryIndexName}([secKeyField(1),... secKeyField(N-1),] AdvancedEnumeratorParam<typeof(secKeyField(N))>)`

List by secondary key also support variants like `ListByAgeVariantName`.

### Count (by secondary index)

    uint|int|long|ulong CountByAge(AdvancedEnumeratorParam<uint> param);
    uint|int|long|ulong CountByAge(uint age);

Count records by specified range `CountBy{SecondaryIndexName}([secKeyField(1),... secKeyField(N-1),] [AdvancedEnumeratorParam<typeof(secKeyField(N))>)]`

### Upgrade

When secondary definition is changed (for example new index is defined) then it is automatically added/recalculated/removed in `InitRelation` call. You can see examples in
[ObjectDbTableUpgradeTest](../BTDBTest/ObjectDbTableUpgradeTest.cs)

## Free content

During removing or updating of data, all IDictionaries and IOrderedSets present in removed data are automatically cleared to avoid data leaks (Also works recursively IDictionaries are freed automatically if they are nested in another IDictionary). You can see examples in
[ObjectDbTableFreeContentTest](../BTDBTest/ObjectDbTableFreeContentTest.cs)

If you have IIndirect property. You are on your own. And that's include any nested IDictionary which needs to be cleared before. So you need recursively load objects and delete them. See test named `IIndirectMustBeFreedManually` in [ObjectDbTableFreeContentTest](../BTDBTest/ObjectDbTableFreeContentTest.cs).

## Modification check during enumeration

When you Insert, RemoveById or insert item using Upsert during enumerating relation an exception will be thrown. It is still possible to modify by Update (or Upsert for existing items) see `CheckModificationDuringEnumerate` in [ObjectDbTableTest](../BTDBTest/ObjectDbTableTest.cs) for details. Modification of secondary indexes during enumerating by secondary indexes are not detected in this moment.
