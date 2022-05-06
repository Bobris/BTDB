# Object DB and Event serialization types support

## Simple types

-   signed integers (`sbyte`, `short`, `int`, `long`) `Constraint<long>`/`Constraint.Signed`
-   unsigned integers (`byte`, `ushort`, `uint`, `ulong`) `Constraint<ulong>`/`Constraint.Unsigned`
-   `bool` `Constraint<bool>`/`Constraint.Bool`
-   `float` (`Single`)
-   `double` (`Double`)
-   `Decimal`
-   `string` `Constraint<string>`/`Constraint.String`
-   `EncryptedString`
-   `DateTime` `Constraint<DateTime>`/`Constraint.DateTime`
-   `DateTimeOffset` (ordering is correct only for identical time offsets, better to use `DateTime` if you need ordering because only UTC is allowed)
-   `TimeSpan`
-   `Guid`
-   `IPAddress`
-   `Version`
-   `byte[]` (if it is last field in ordering it is lexicographically sorted)
-   `StringValues` from Microsoft.Extensions.Primitives

## Complex types

-   class with public properties with getter and setter. Annotate with `[NotStored]` to skip property.
-   `IIndirect<T>` In Object DB is it not stored inline and it has its own `Oid`, lazily loaded. In Event serialization it is skipped by default, but could be configured to store it too (always as inline).
-   `IList<T>`, `List<T>`, `ISet<T>`, `HashSet<T>` (Inline list of items only for smaller amount of items, set versions deduplicate items on deserialization)
-   `Dictionary<TKey,TValue>` (Inline map useful only for smaller amount of items)
-   `IDictionary<TKey,TValue>`, `IOrderedDictionary<TKey,TValue>` (Lazy loaded, ordered by TKey map, good for bigger number of items, and can be iterated in order)
-   `IOrderedSet<T>` (Lazy loaded, ordered by T set, good for bigger number of items, and can be iterated in order, don't use in Event serialization, do not use `OrderedSet<T>` (only as initial constructor of content))

## Default conversions on load

-   T to IList<T> when T is ValueType then List<T> has always 1 item, when T is null then List<T> has zero length.
