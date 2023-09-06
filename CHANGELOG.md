# Changelog

## [unreleased]

### Breaking change / Improvement

IOC does not use Runtime IL code generation, but SourceGenerator. Some features were lost, some features could be sill added back if there will be demand. Assembly scanning was changed most. And all resolvable classes had to be marked by `[Generate]` attribute on itself or any parent classes or interfaces.
It does allow to use IOC in AOT builds. More features will follow.

## 31.11.3

### Fixed

Upgrading to nullable property in secondary key.

## 31.11.2

### Fixed

Now "ById" methods should really work.

## 31.11.1

### Fixed

InKeyValues should not be required in most "ById" methods.

## 31.11.0

### Added

Validation that InKeyValues must be after all PrimaryKeys.

### Fixed

Regression ShallowUpdate when value is complex.

## 31.10.0

### Added

New more succinct way to declare InKeyValue.

## 31.9.0

### Fixed

Bug in ReadOnlySequenceSpanReader.ReadBlock.

### Added

Huge feature in relations InKeyValue allowing optimize modification and search of small number of fields into Primary Key. Documentation will come in future commit.

`OnBeforeRemove` attribute to help to implement foreign keys prevent removal or cascading delete.

## 31.8.0

### Removed

Now DB trl files are never resized, so method `SetSize` is never used now and its implementation does not matter.

### Optimized

Speed of Compactor scanning all used files.

## 31.7.0

### Added

Async versions of DB Import and Export.

## 31.6.0

### Added

XXHash64 implementation, very fast noncryptographic hash. Fletcher checksum slightly optimized.

## 31.5.2

### Fixed

Change type of db field from enum to nullable enum does not report upgrade incompatibility.

## 31.5.1

### Fixed

ReadString and SkipString could rarely randomly crash. (had pointer instead of managed reference without pinning buffer)

## 31.5.0

### Added

`BonBuilder.builder.EstimateLowerBoundSize()` method.

## 31.4.3

### Fixed

IOC: Lazy Singletons were not threadsafe

## 31.4.3

### Fixed

# IOC: Lazy Singletons were not threadsafe

### Added

`BonBuilder.builder.EstimateLowerBoundSize()` method.

> > > > > > > e7279773... BonBuilder.builder.EstimateLowerBoundSize()

## 31.4.2

### Fixed

`ReadOnlySequenceSpanReader`

## 31.4.1

## 31.4.0

### Added

`ReadOnlySequenceSpanReader`

### Improved

ODbDump `leakscode` has more memory optimal variant db on disk to reduce memory usage on big db's

ODbDump `trldump` command outputs statistic about used space

## 31.3.2

### Fixed

In EventLayers Nested ValueTuples containing objects generated invalid IL code.
`SpanReader.ReadByteArrayAsMemory()` didn't worked correctly with `SpanReader.Controller!=null`.

## 31.3.1

### Improved

Diagnosing EventLayer serialization exceptions also for another another case.

## 31.3.0

### Added

`Constraint<Guid>.Any` is supported.

## 31.2.1

### Fixed

ReadOnlyMemory<byte> was not working in EventLayers.

## 31.2.0

### Improved

Diagnosing EventLayer serialization exceptions also for another case.

### Optimized

SpanReader.ReadStringOrdered is now up 15 times for long ascii only strings.

## 31.1.1

### Improved

Diagnosing EventLayer serialization exceptions.

## 31.1.0

### Added

CustomConverter now could be defined for EventLayer and it works for class types in both EventLayer and EventLayer2.

## 31.0.0

### Optimized

Decreased Compaction memory usage.

SpanReader.SkipStringOrdered is now 80+ times faster for long ascii only strings.

## 30.5.0

### Added

CustomConverter for Relations could be set even after opening DB.

## 30.4.0

## 30.3.0

### Added

Exposed `CompactorReadBytesPerSecondLimit` and `CompactorWriteBytesPerSecondLimit` properties so they could be modified on the fly.

## 30.2.2

### Fixed

Regression in WriteString allocating too much memory if called multiple times.

## 30.2.1

### Fixed

Regression from 30.2.0 in SkipString.

### Optimized

SpanWriter.WriteStringOrdered is now faster especially for ascii only strings.

## 30.2.0

### Optimized

SpanWriter.WriteString is now 12+ times faster for long (100+) ascii only strings.
SpanReader.ReadString is now up to 14+ times faster for long ascii only strings.
SpanReader.SkipString is now 100+ times faster for long ascii only strings.

### Added

Support for `ReadOnlyMemory<byte>` type and end to end sample to stream Key,Bon relation enumerable though Stream.

## 30.1.0

### Added

New Bon constructor.

### Fixed

Very complex case of calling Remove before any deserialization on object with IDictionary changed to Dictionary.

### WIP

ReadOnlyDatabase = wait for final version.

## 30.0.0

### Breaking changes

Requires C#11 and .Net 7.0

## 29.12.1

### Fixed Regression from 29.12.0

Allow to register interfaces not just classes.

## 29.12.0

### Added

New OnSerializeAttribute to mark method on relation item to be run before `Insert`, `Upsert`, `Update` and all their variants.

DB now throws exception when scalar value is tried to be stored as inline object (it didn't worked anyway).

DB Iterator notify and skips about missing values in Relations. ODbDump `dumpnull` prints number of such missing values.

New function `CalcBTreeStats` to calc count of node sizes.

ODbDump has new command `leakscodeapply` which enable apply output file from `leakscode` command on db (then can be compared dump before and after clean - must be the same)

ODbDump `check` command was improved to not stop on first error, but to count all corrupted pairs.

ODbDump has new `fix` command to erase all corrupted pairs and also clear all secondary indexes so they could be rebuild on next app start.

## 29.11.0

### Added

Exposed property `IObjectDB.AutoRegisterTypes`.

New method `(ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) IKeyValueDB.GetNativeMemoryStats();`.

## 29.10.1

### Fixed

FirstBySecondaryKey throwed when secondary key was modified new version.

## 29.10.0

### Added

New relation method `void RemoveAll()`, you should guess what it does.
New relation method `UpsertRange` and `As<T>`.
`OnCreate` callback for relations which helps to write upgrade methods.

### Optimized

VUInt pack unpack calculations.

## 29.9.1

### Fixed

Bug in ScanBy with Constraint.First (didn't properly skipped nonfirst) when used on last property of key.

## 29.9.0

### Added

Relations new FirstByIndex(OrDefault) methods. It is similar to GatherBy just sorting is much faster because it does not need to be sorted.

`Orderer`.`GenericAscending` and `Orderer`.`GenericDescending` methods which just don't check property owner type equality with relation item type.

`Bon` Binary object notation is allows creating and reading JavaScript/C# values with extensions like Dictionary/Map into binary notation. It is much faster to parse, write, skip, search by keys than JSON, size will be also smaller in most cases, in some cases much more smaller.

## 29.8.0

### Added

Non-generic `RegisterFactory` for your low level needs.

## 29.7.0

### Added

EventSerializer layer2 has new option to ignore known descriptors on input instances.

## 29.6.0

### Added

Generic `Constraint.First(ofConstraint)` - takes only first row from every group.

## 29.5.0

### Added

New `IKeyValueDBLogger` callback `ReportCompactorException`. Needs override with better logging of exception in production applications.

## 29.4.0

## 29.3.0

### Added

Locale/Culture based orderer (`Orderer.AscendingStringByLocale`).
Also added `Orderer.Backwards(orderer)` order modifier.

## 29.2.0

### Fixed

Force ReferenceEquality when comparing objects serialized in DB.

## 29.1.1

### Fixed

Skipper from previous version generated invalid IL code which make violent crash, not in tests though :-(

## 29.1.0

### Changed

Unregistered object inside another object is skipped without throwing exception.

### Added

Improved ODbDump diskdump and trldump commands.

## 29.0.0

### Added

New RequireContentFree attribute to mark class to always use detection of IDictionaries on it to prevent Data Leaks.

Allow to use Brotli compression for Kvi. For now disabled by default. Added new parameter to logger so it is breaking change, but easily fixable.

## 28.3.2

### Fixed

Previous fix was incomplete.

## 28.3.1

### Fixed

Kvi in Native Heap KVDB must be created in Exclusive mode.

## 28.3.0

### Added

Upgrade from IDictionary<K1,T1> to Dictionary<K2,T2> is possible and tested in Relations.

## 28.2.0

### Added

Enum Constraints.

Generic `Constraint<T>.Any` (not really generic just supporting all currently available constraints (bool, string, DateTime, (un)signed, Enum))

## 28.1.2

### Fixed

ScanBy and GatherBy secondary index when DB is upgraded with secondary index removed.

## 28.1.1

### Fixed

Enums with multiple synonym labels for the same value (for example HttpStatusCode 300) can be stored

## 28.1.0

### Fixed

Loose Constraint on first position didn't found all matched records.

### Added

Relations `UpdateById` method for faster updates.

## 28.0.0

### Added

UpTo Constraints, which quickly skip rest of values.

## 27.0.3

### Fixed

ScanBy of empty relation does not throw Exception anymore.

## 27.0.2

### Fixed

Hopefully last bug in native heap fixed.

## 27.0.1

## 27.0.0

### Breaking Change

Added support for .Net 6.0. Removed support for .Net 5.0.

## 26.14.0

### Added

Constraints and ScanById relation method.

### Changed

Enum compatibility is now always like it was with BinaryCompatibilityOnlyAttribute. Please remove all usages of this attribute from client code as it does not have any meaning.

## 26.13.0

DefaultTypeConvertorGenerator now supports `T` to `T?` conversion. Also `U` to `T?` if `U` is convertible to `T`.

## 26.12.1

Fixed native heap implementation.

## 26.12.0

## 26.11.0

### Added

A lot of important stuff in ObjectDB are public now.
Expanded interactive mode in ODbDump.

## 26.10.0

### Added

Relations now supports Items without default constructor.

## 26.9.1

### Fixed

Iterate only strictly abstract methods in Relation interfaces.

## 26.9.0

### Added

Name in DBOptions and show it in interesting exceptions. Same Name is also used for Key in deduplication of Relations.

## 26.8.0

### Added

Protection against using singletons for database objects or relation items.

## 26.7.0

### Added

ObjectDB+Relations+EventLayers support for Tuple and ValueTuple.

### BREAKING CHANGE

During enumeration of relation range is prevented also modification of secondary keys (until now was possible)

## 26.6.0

### Added

IOC can enforce uniqueness of registrations.

## 26.5.0

### Added

EventStore2Layer now correctly serializes generic classes.

`dynamic` deserialization of object now implements `IEnumerable<KeyValuePair<string, object>>` for easy iteration of over all values and fields. `ITypeDescriptor` has new property `IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields { get; }` which returns list of all fields on Object descriptor, for all other descriptors it returns empty list.

## 26.4.0

### Added

Optimization to skip reading value in relations when partial view is subset of primary key.

## 26.3.1

### Fixed

Tried to workaround rare problem with compactor corrupting DB.

## 26.3.0

### Removed

Support for .NetCore 3.1. Only .Net 5.0 is supported now.

### Added

ODbDump has new command `fulldiskdump` containing also content of secondary keys

ODbDump has new command `trldump` listing operations in .trl files

New interface `IFieldHandlerLogger` for logging type incompatibilities. Sample usage `new DBOptions().WithFieldHandlerLogger(new DefaultFieldHandlerLogger(s => Console.WriteLine(s)))`.

Possibility to iterate all transactions. Each transaction also has new getter `CreatedTime` and `IsDisposed()`.

## 26.2.2

### Fixed

Relations GetEnumerator didn't reset enumerator correctly in all cases.

## 26.2.1

### Fixed

Random crash in IsFreeContentNeeded due to multiple threads could run this in parallel.

## 26.2.0

### Added

In debug mode throws when using in listing by partial key excluded start proposition. In release mode kept current behavior.

### Fixed

IOC Nullable values as instance dependencies.

## 26.1.1

### Fixed

Deleting indirect will not throw when value is null.

## 26.1.0

### Added

ODbDump has new command `findsplitbrain` which can find first commit when content of relation in two instances of DB diverted - DBs needs to contain enough preserved history

`IRelation` has new `SerializeInsert` method which allows speed up DB insert by preparing simple CreateKeyValue commands ahead of writing transaction.

## 26.0.0

### Fixed

Compactor sometimes removed unused files too late.

### Removed

`ReadOnlyListArrayWrapper` not used internally anymore and it is too confusing.

### Added

New methods`TransactionLogCreated` and `FileMarkedForDelete` in `IKeyValueDBLogger` for logging file collection operations

## 25.5.0

### Fixed

PositionLessStreamProxy supports also stream which does not read all bytes, but are not at the end.

## 25.4.0

### Added

Default conversion from `T` to `IList<T>`.

## 25.3.0

### Fixed

Regression with Kvi file contain garbage at end. Enable `LenientOpen` to allow open DB with this corruption.

## 25.2.0

### Added

netcoreapp3.1 framework could be targeted again.

## 25.1.0

### Added

`LenientOpen` to `KeyValueDBOptions` (default false), in strict mode it does not try to recover as much as possible data, but starts with empty DB. When such problem is detected it will call new method `LogWarning` from `IKeyValueDBLogger`.
Also you can newly pass implementation of `IKeyValueDBLogger` into `Logger` field in `KeyValueDBOptions`.

## 25.0.1

### Fixed

Regression in SpanReader.CheckMagic which caused incomplete DB open in some cases.

Detect transaction leaks for writing transactions as well.

## 25.0.0

### Breaking change

Whole code was spanificated and cleaned from old unused in production parts.

### Added

IOC Container Build can verify that Singletons does not depend on transient objects.

## 24.8.0

### Added

Some missing IL Helpers.

`Dependency` attribute inheriting from `NotStored` attribute.

## 24.7.0

### Added

Support for serialization of type `StringValues` from Microsoft.Extensions.Primitives.

Support for serialization of properties with private setter in base class.

## 24.6.1

### Fixed

EventLayer Dictionary, List deserialization didn't reset variable, so instead of null there could be previous reference when nested in another List or Dictionary.

## 24.6.0

### Added

Support for `DateTimeOffset` type in serialization.

## 24.5.0

### Added

IL: Allow to define parameter names on constructor so it is usable in IOC.

### Fixed

IOC: Do not crash on constructors without defined parameter names.

## 24.4.0

### Added

Relations: Skip already implemented methods in Relation interface.

## 24.3.0

### Added

ObjectDB: Generic classes supported as DB objects.

## 24.2.0

### Added

ObjectDB: Classic DB objects and Singletons could be created by IOC.

ObjectDB: Iterator decrypting encrypted strings.

## 24.1.0

### Added

IOC: `ResolveOptional` new method works like in Autofac.

Relations: ObjectDB in DB options allow to specify IOC Container which is then used for creating items in relation with fallback to simple new if not registered.

## 24.0.2

### Fixed

Ignore static properties on DB Objects.

## 24.0.1

### Fixed

KeyValueDB: Value of length exactly 7 bytes are in memory again.

## 24.0.0

### Fixed

Relation self healing when primary key is changed works in more cases.

### Added

Made public Owner getters in Transactions. Allowed to change Max Transaction Log size using public property.
Some performance optimizations.

## 23.1.0

### Added

Relations throws informative exception when trying to use unsupported RemoveBySecondaryKey.

### Fixed

Performance regression in RemoveById when type didn't contain any IDictionaries.
Failure to cast in FreeContent in ODBDictionary.

## 23.0.0

### Breaking changes

- Visitor StartRelation gets whole info instead of just name of relation.
- Public fields on Relation Rows and Database objects are forbidden unless they have `[NotStored]`. In next version they could became supported.

## 22.2.2

### Fixed

- Final fix for IDictionaries

## 22.2.1

### Fixed

- IDictionaries has uniqueness fix also for keys.
- RelationInfo.GetProperties skips "NextInChain" IRelation property.

## 22.2.0

Important note: Don't forget to commit Transactions which calls GetRelation first time (auto registering them).

### Added

- `ICovariantRelation<out T>` cannot have upsert, but it will implement `IRelation<T>` anyway.
- removed class constraint because it created strange problems. It is now enforced in runtime.

### Fixed

- IDictionaries now supports types with same name but different namespaces again.

## 22.1.0

### Added

- `IsReadOnly()` on `IKeyValueDBTransaction` and used internally in auto registering Relations directly not just in writing transactions, but also in all non read only.

## 22.0.0

### BREAKING CHANGE

- GetRelation(Type type) now returns IRelation type
- RelationInfo public getters changed many types to ReadOnlyMemory.
- Removed useless UniqueIndexAttribute

### Added

- big chunk of IL generated code for Relations and ODBDictionary/Set is now cached forever, making it faster to instantiate Relations repeatedly. It adds new limitation that instances of `ITypeConvertorGenerator` and `IFieldHandlerFactory` needs to be same over process runtime.

## 21.0.0

### BREAKING CHANGE

- Relations interfaces needs to be inherited from `IRelation<T>`

### Added

- `IRelation<T>` and `IRelation` interfaces. You will get `Upsert` method for free.

```C#
    public interface IRelation<T> : IReadOnlyCollection<T>, IRelation where T : class
    {
        bool Upsert(T item);
    }

    public interface IRelation
    {
        Type BtdbInternalGetRelationInterfaceType();
        IRelation? BtdbInternalNextInChain { get; set; }
    }
```

- `IObjectDbTransaction` has new methods

```C#
    object GetRelation(Type type);

    T GetRelation<T>() where T : class, IRelation
    {
        return (T)GetRelation(typeof(T));
    }
```

These lazily creates instance of relation for current transaction. If it is for first time it will also create it (in current transaction if it is writable, or in new writable transaction). It automatically names relation by `T.ToSimpleName()` or uses `PersistentName` attribute on `T`.

You can register your own custom relation factory by using `void IObjectDB.RegisterCustomRelation(Type type, Func<IObjectDBTransaction, object> factory);`.

You can forbid automatic registration of relations by `IObjectDB.AllowAutoRegistrationOfRelations = false`. Good in production code to allow auto registration only during initial transaction.

### Fixed

- regression in 20.x in compatibility of Enums in relations with `BinaryCompatibilityOnly` attribute.

## 20.3.0

### Added

- ReadOnly option to opening DB. ODbDump using it and allows to pass ulongcommit as third parameter to open DB in historical moment.

### Fixed

- Mixing Lists, Sets and Arrays in EventLayers

## 20.2.0

### Added

- IOC now support `Dependency` attribute for properties injection. Also it could be used for renaming dependency resolved name. Nullable reference types are optional dependencies.

## 20.1.0

### Added

- IOC now supports public properties injection. Registration needs to be done with `PropertiesAutowired()`. Setters does not need to be public. Nullable reference types are optional dependencies, all other properties are required.

## 20.0.0

### Added

- Added support for `IOrderedSet<T>` lazily stored set.
- EventLayers deserialization can now unwrap `IIndirect<T>`, making it compatible change (`IIndirect<T>` => `T`, or `IDictionary<TKey, IIndirect<T>>` => `IDictionary<TKey, T>`).
- New documentation for [supported types](Doc/SupportedTypes.md)
- Added support for `ISet<T>`, `HashSet<T>` with identical serialization as `IList<T>`.
- Removed some allocations from `IOrderedDictionary`

## 19.9.3

### Fixed

- Regression with DB loading `IDictionary<Key,IIndirect<SomeAbstractClass>>`

## 19.9.2

## 19.9.1

### Fixed

- Regression from 19.8.0 with NullReferenceException in some special cases.

## 19.9.0

### Added

- Suffixes for partial deserializations in methods (FindBy,ListBy) does not need to be separated by underscore anymore.

## 19.8.0

### Added

- Relations now support returning only partial classes. For example it allows to speed up table scanning because you can deserialize only fields you need when enumerating relation.

## 19.7.1

### Fixed

- Made AesGcmSymmetricCipher thread safe.

## 19.7.0

### Added

- Support `EncryptedString` in DB indexes (orderable).

## 19.6.0

### Added

- New `EncryptedString` type to be able to store string in its encrypted form. You need to pass `ISymmetricCipher` implementation to `DBOptions`, `TypeSerializersOptions`, `EventSerializer` and `EventDeserializer`. There is class `AesGcmSymmetricCipher` implementing `ISymmetricCipher`, which provides perfect security by just passing 32 bytes key to its constructor.

## 19.5.0

- Added possibility to deserialize event with Nullable to dynamic (usable for dumping EventStore)

## 19.4.0

### Added

- All serializations DB, Event now supports `System.Version` type. Default conversion allows to upgrade from `string` to `Version`. When `Version` is used in ordering, keys it behaves as expected.

### Fixed

Removed control flow by exceptions from `EnumerateSingletonTypes`. Fixes #85.

## 19.3.0

### Added

Relations new methods `AnyById` and `AnyBy{SecKeyName}` supported.

## 19.2.0

### Added

#### Relations

- New methods `CountById` and `CountBy{SecKeyName}` supported.
- `IEnumerator` and `IEnumerable` could be freely exchanged as result types.
- `ListById` and `ListBy{SecKeyName}` does not require `AdvancedEnumeratorParam`.

## 19.1.0

### Changed

Range defined by EndKey s KeyProposition.Included now contains all keys with passed prefix, used in methods: `RemoveById` `ListById` `ListBy{SecKeyName}`

## 19.0.0

### Breaking change

Needs to be compiled with in csproj:

    <LangVersion>8</LangVersion>
    <Nullable>annotations</Nullable>

`StructList.Add()` renamed to `AddRef()`.

### Added

IOC: IAsyncDisposable is not registered by AsImplementedInterfaces (same behavior as IDisposable).

## 18.2.2

### Fixed

Another bug in FindLast made me to delete it and rewrite again from managed heap implementation.

## 18.2.1

### Fixed

Bug in FindLast in native heap KVDB.

## 18.2.0

### Added

EventLayers Deserialization now supports classes without parameter-less constructor.

## 18.1.0

### Fixed

Find with prefix sometimes found records not matching prefix.

### Added

Improved CalcStats information.
Some small speed optimizations.

## 18.0.0

### Changed

Supports only .Net Core 3.0 or better.

### Added

New BTreeKeyValueDB implementation which uses native heap.

## 17.10.0

### Added

New method in relations: ShallowRemoveById
StartWritingTransaction returns ValueTask and optimized allocations.

## 17.9.0

### Added

ODBIterator extended to be able to seek and display only what is needed.

## 17.8.0

### Added

New method `IPlatformMethods.RealPath` for platform independent expanding of symlinks.

## 17.7.0

### Added

New method `ByteBuffer ByteBuffer.NewAsync(ReadOnlyMemory<byte> buffer)`.

FullNameTypeMapper improved support for generics. Types can migrate assemblies even for generic arguments. (by <https://github.com/JanVargovsky>)

### Changed

Supports only .Net Core 2.2 or better.

## 17.6.0

### Fixed

- When preserving history KVDB did not advising compaction without restarting application.

### Changed

- Default CompactorScheduler wait time to 30-45 minutes.

## 17.5.2

## 17.5.1

### Fixed

- Compactor does not ends in endless cycle when DB is opened with more than 4 times smaller split size than it was created.

## 17.5.0

### Added

ODbDump has new commands

- `leaks` which prints out unreachable objects in DB.
- `frequency` which prints number of items in relations and top level dictionaries in singletons

### Fixed

ODBLayer correctly supports interfaces in properties.

## 17.4.2

### Fixed

Additional nonderministic info removed from compare mode of ODbDump.

## 17.4.1

### Fixed

ODbDump is now published in way it works not just on my machine.

## 17.4.0

### Added

ODbDump is now part of release. ODbDump has new dump mode useful for comparing DBs.

## 17.3.0

RemoveById supports advanced enumeration param in relations

## 17.2.0

### Added

Extend TypeSerializers with optional configuration options.

Options consist of one option for `IIndirect<T>`, whether it is serialized or ignored.

## 17.1.0

### Added

Way to limit Compactor Write and Read Speed by setting `KeyValueDBOptions`. Default is unlimited.

## 17.0.1

### Changed

Added new method into IFileCollectionFile.AdvisePrefetch. It is called during DB open on files which are expected to be read by RandomRead.

## 17.0.0

### Changed

IFileCollection modified to allow faster implementations possible.

## 16.2.1

### Fixed

PRead behavior on Windows file end. Fixed Nested Dictionaries type gathering exception.

## 16.2.0

### Fixed

Failure to open DB in special case after erasing and compaction.
Type check when generating apart fields in relations.

## 16.1.0

### Added

Reintroduced PossitionLessStream and rename FileStream one to PossitionLessFileStream

## 16.0.0

### Improved

Much faster compaction when a lot of changes were done. New IKeyValueDB.CompactorRamLimitInMb does limit RAM usage for longer time.
Speed of OnDiskFileCollection improved by using new PRead and PWrite methods implemented for Windows and Posix.
Better exception in WriteInlineObject when object type could not be stored.

### Breaking Changes

Modified IKeyValueDBLogger and IKeyValueDB so implementation needs to be modified.

### Fixed

Skipping Events in EventStoreLayer Deserialization

## 15.1.0

### Added

Added way to skip Events in EventStoreLayer Deserialization.

### Fixed

Deletion of dictionaries during update/delete in relation in subclasses when not defined in declaration by interface.

## 15.0.0

### Added

ShallowUpsert and ShallowUpdate relation methods which does not try to prevent leaks, but are much faster.

### Changed

IIndirect objects are not automatically deleted during removal from relations.

## 14.12.2

### Fixed

Calling ListBy{SecondaryKey}OrDefault for not existing item during enumerating relations cooperates well.

## 14.12.1

### Fixed

Exception in EventStore2Layer serialization does not corrupt next serializations anymore.
Serialization of non Dictionary in EventStore does not fail.

## 14.12.0

### Fixed

Skipping removed field (inline object) when deserializing older version in relations

## 14.11.0

### Added

EventLayer serializers support IOrderedDictionary<K,V> type

## 14.10.0

### Added

ArtInMemoryKeyValueDB - less memory hungry KVDB - use it only in .NetCore 2.1 target
Generics classes now supported in EventLayer serializers
DBOptions.WithSelfHealing switches db to try self heal rather then fail fast mode
IObjectDBLogger for ObjectDB, actually for reporting deletion of incompatible data in self heal mode.

## 14.9.0

### Fixed

Rare exception during checking possibility of usage of optimized version of prefix based remove when so far unseen objects was used as key in IDictionary

## 14.8.0

### Changed

Dumping JsonLike output from TypeDescriptor is now more JSON compliant.

## 14.7.0

### Added

Delegate constrains are now supported in C# 7.3, so it now makes compile time errors instead of runtime where possible.

### Fixed

Rare failure in IOC when running in parallel.
Better exception message when types of fields in Deserialization are different.

## 14.6.0

### Added

Relations could be now iterated.

## 14.5.2

### Fixed

Event Deserialization does not eagerly require Types exists in List and Dictionary.
Iterator fix for back reference in inlined lists and dictionaries. Now really works ;-)

## 14.5.1

### Fixed

Shut up Coverity. DiskChunkCache findings are false positives.
Iterator fix for back reference in inlined lists and dictionaries.
EventStore2Layer Deserializer bug in specific case.

## 14.5.0

### Added

Allow to use DateTime.MinValue and DateTime.MaxValue in ordered context - they will be automatically converted to UTC.

## 14.4.0

### Fixed

ListBy... methods in relations now correct type of AdvancedEnumeratorParam<T>

## 14.3.0

### Fixed

Made order of properties in EventSerializers stable by sorting them by name.

## 14.2.1

### Fixed

EventLayer2 serialization of `Dictionary<int, Dictionary<int, ComplexObject>>` property.

## 14.2.0

### Changed

IOC RegisterInstance<T>(object value) now must be explicitly used so new RegisterInstance(object value) could be used.

## 14.1.0

### Added

IOC RegisterInstance(object value) overload.

## 14.0.0

### Added

RollbackAdvised property on KV and Object transactions interfaces to simplify notification of some infrastructure code to rollback transaction instead of committing it.

Relations support inheriting of methods from other interfaces.

### Breaking change

IOC RegisterInstance<T>(T value) now allows also value types as T, and value is not registered as value.GetType() but as typeof(T), which is same behavior as AutoFac.

## 13.1.0

### Added

Possibility to limit number of removed items at once in prefix based remove in relations.

## 13.0.0

### Added

Ported to .NetCore 2.0.

Simplified Nuget. Now it embed pdb and with SourceLink. GitHub releases contains zipped BTDB sources.

New releaser project to automate releasing.

## 12.7.0.0

### Fixed

Compactor Inducing Latency for writting transaction is now capped.

## 12.6.1.0

### Fixed

Fixed Nullable support in EventStore2Layer.

## 12.6.0.0

### Fixed

Uninterupted influx of data after db opening can prevent compactor from running.

## 12.5.1.0

### Fixed

IPAddress can now serialize and deserialize null value.

## 12.5.0.0

### Added

- Synchronization lock in EventLayer2 Deserialization to be on safe side.

## 12.4.0.0

### Added

- PersistedNameAttribute is supported on Apart Fields in relation interfaces

## 12.3.0.0

### Added

- IOC now resolves optional parameters that are not registered with its provided value

### Fixed

- Fixed problem with calculating index from older version value in specific case

## 12.2.0.0

### Added

- new method DeleteAllData() on ObjectDBTransaction
- PersistedNameAttribute is additionally allowed on interfaces - useful for Relations

## 12.1.0.0

### Added

- Event deserialization now automatically converts Enums to integer types.

## 12.0.0.0

### Added

- Changelog
- Nullable support in both ODb and EventStore
