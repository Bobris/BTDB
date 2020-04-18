# Object DB and Event serialization types support

## Simple types

- signed integers (`sbyte`, `short`, `int`, `long`)
- unsigned integers (`byte`, `ushort`, `uint`, `ulong`)
- `bool`
- `float` (`Single`)
- `double` (`Double`)
- `Decimal`
- `string`
- `EncryptedString`
- `DateTime`
- `TimeSpan`
- `Guid`
- `IPAddress`
- `Version`
- `byte[]` (if it is last field in ordering it is lexicographically sorted)

## Complex types

- class with public properties with getter and setter. Annotate with `[NotStored]` to skip property.
- `IIndirect<T>` In Object DB is it not stored inline and it has its own `Oid`, lazily loaded. In Event serialization it is skipped by default, but could be configured to store it too (always as inline).
- `IList<T>`, `List<T>`, `ISet<T>`, `HashSet<T>` (Inline list of items only for smaller amount of items, set versions deduplicate items on deserialization)
- `Dictionary<TKey,TValue>` (Inline map useful only for smaller amount of items)
- `IDictionary<TKey,TValue>`, `IOrderedDictionary<TKey,TValue>` (Lazy loaded, ordered by TKey map, good for bigger number of items, and you can be iterated in order)
