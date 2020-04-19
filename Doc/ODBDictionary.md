# ObjectDB Ordered types

## Ordered Dictionary

Next examples expects to get somehow transaction. It could be done like this:

    using BTDB.KVDBLayer;
    using BTDB.ODBLayer;

    IKeyValueDB kvDb = new InMemoryKeyValueDB();
    IObjectDB db = new ObjectDB();
    db.Open(kvDb, false); // false means that dispose of IObjectDB will not dispose IKeyValueDB
    IObjectDBTransaction tr = db.StartTransaction();

Let's first define our dictionary in some singleton type: (note I defined value type of Dictionary as StoredInline)

    [StoredInline]
    public class User
    {
    	public string Name { get; set; }
    	public ulong Age { get; set; }
    }

    public class Root
    {
    	public IOrderedDictionary<ulong, User> Id2User { get; set; }
    }

Now it is easy to get reference of Singleton because we have `IObjectDBTransaction`:

    var root = tr.Singleton<Root>();
    var dict = root.Id2User;

How to add new User? (It will throw if already exists)

    dict.Add(1, new User { Name="Boris", Age=12345 });

How to update User? (It will add it if does not exist)

    dict[1] = new User { Name="Boris", Age=54321 };

How to remove User?

    dict.Remove(1);

How to remove All users?

    dict.Clear();

How to remove range of users? (remove all from 1 to 9)

    dict.RemoveRange(1, true, 10, false)

How to enumerate all users?

    foreach (var pair in dict)
    {
    	Console.WriteLine($"Id: {pair.Key} Name: {pair.Value.Name}");
    }

How to enumerate users skipping 20, and reading only 10, and printing total number of users:

    var enumerator = dict.GetAdvancedEnumerator(new AdvancedEnumeratorParam<ulong>());
    Console.WriteLine($"Total number of users: {enumerator.Count}");
    enumerator.Position = 20;
    var counter = 0;
    ulong key;
    while (counter++<10 && enumerator.NextKey(out key))
    {
    	Console.WriteLine($"Id: {key} Name: {enumerator.CurrentValue.Name}");
    }

AdvancedEnumeratorParam has options to limit range of iteration and define in which order you want to iterate that range. As example next iterator will iterate from id 19 down to id 10.

    new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Descending,10,KeyProposition.Included,20,KeyProposition.Excluded)

## Ordered Set

- useful for bigger ordered sets

      	public IOrderedSet<string> Something { get; set; }

If you want to create new instance before initializing new instance by DB:

    obj.Something = new OrderedSet<string> { "A", "B" };

`OrderedSet<T>` is just `HashSet<T>` where all new methods from `IOrderedSet<T>` throw `NotSupportedException`. After you store this instance to DB and retrieve it again it will actually return `ODBSet<string>` instance which behaves like `IOrderedDictionary<T,void>`.
