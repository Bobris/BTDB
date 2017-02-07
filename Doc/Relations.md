# Relations #

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

## Basic operations ##
When defined in interface following methods are automatically implemented by BTDB when defined in relation interface

### Insert ###
       personTable.Insert(new Person { Id = 2, Name = "admin", Age = 100 });
 will throw if already exists

### Update ###
       personTable.Update(new Person { Id = 2, Name = "superadmin", Age = 100 });
will throw if does not exist

### Upsert (Insert or Update) ###
		var inserted = personTable.Upsert(new Person { Id = 2, Name = "superadmin", Age = 100 });
return true if inserted

### Remove ###
	(void|bool) RemoveById(primaryKey1, ..., primaryKeyN);
Returns true if removed, void variant throw when does not exists. All primary keys fields are used as parameters, for example	`void RemoveById(ulong tenantId, ulong userId);`

	int RemoveById(primaryKey1 [, primaryKey2, ...]);
Returns number of records removed for given primary key prefix (apart fields are automatically used)
for example	`void RemoveById(ulong tenantId)` removes all users for given tenant


### Find ###
            Person FindById(ulong id);
It will throw if does not exists, as parameters expects primary key fields (same as in RemoveById)
            
            Person FindByIdOrDefault(ulong id);
Will return null if not exists
            
            IEnumerator<T> FindById(primaryKey1 [, primaryKey2, ...]);
Find all items with given primary key prefix (apart fields are automatically used)

            Person FindByAgeOrDefault(uint age);
Find by secondary key, it will throw if it find multiple Persons with that age. Note "Age" in the name is name of secondary key index.

            IEnumerator<Person> FindByAge(uint age);
Find all items with given secondary key.

### List ###
		IEnumerator<Person> ListById(AdvancedEnumeratorParam<uint> param);

List by ascending/descending order and specified range. Apart fields are taken into account for listing by primary key. Parts of primary key may be used for listing. In example below you can list all rooms or just rooms for specified company by two ``ListById`` method. (Both ``IOrderedDictionaryEnumerator`` and ``IEnumerator`` can be used as return values.)

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

### Enumerate ###
            IEnumerator<Person> GetEnumerator();
Enumerates all items sorted by primary key. Current value of apart field in table interface is not taken into account and all items are enumerated. Enumerated are all always all items, apart fields are not taken into account.  

### IReadOnlyCollection ###
All relations implements `IReadOnlyCollection<T>`. This can be used during debugging immediately, or directly in code - after casting or defining like this: `public interface IRoomTable : IReadOnlyCollection<Room>`. Enumerated are all always all items, apart fields are not taken into account.

## Primary Key ##
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

## Apart Field ##
From the example above is obvious that tenantId would be present in many places, to avoid that, it is possible to put such field into the interface like this:

	public interface IPersonTable
    {
        ulong TenantId { get; set; }            
        Person FindById(ulong id);
        bool RemoveById(ulong id);
	    ...
	}

and then use for all operations:

	var personTable = creator(tr);
    personTable.TenantId = 42;
	personTable.Insert(new Person{ Id = 100 }); //TenantId 42 will be used also for insert
    personTable.RemoveById(100);
	personTable.RemoveById(101);
	 

## Secondary Key ##
Secondary keys are usefull for fast access by other fields then primary key. Declared are as attribute `SecondaryKey`. Each secondary index has it's name (may be different then existing fields names). Secondary index may be compound from several fields. Each field can be part of more than one secondary key. for example:

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

### List (by secondary index)###
		IEnumerator<Person> ListByAge(AdvancedEnumeratorParam<uint> param);

List by ascending/descending order and specified range, see `CanIterateBySecondaryKey` in [ObjectDbTableTest](..\BTDBTest\\ObjectDbTableTest.cs)
``ListBy{SecondaryIndexName}([secKeyField(1),... secKeyField(N-1),] AdvancedEnumeratorParam<typeof(secKeyField(N))>)``


### Upgrade ###
When secondary definition is changed (for example new index is defined) then it is automatically added/recalculated/removed in `InitRelation` call. You can see examples in 
[ObjectDbTableUpgradeTest](..\BTDBTest\ObjectDbTableUpgradeTest.cs)

## Free content ##
During removing or updating of data, all IDictionaries present in removed data are automatically cleared to avoid data leaks. You can see examples in
[ObjectDbTableFreeContentTest](..\BTDBTest\ObjectDbTableFreeContentTest.cs)

### StoredInline ###
All data except IDictionaries are by default stored inline - it is not necessary to mark objects as `[StoredInline]`