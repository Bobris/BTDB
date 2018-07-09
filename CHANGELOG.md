# Changelog

## [unreleased]

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

-   Synchronization lock in EventLayer2 Deserialization to be on safe side.

## 12.4.0.0

### Added

-   PersistedNameAttribute is supported on Apart Fields in relation interfaces

## 12.3.0.0

### Added

-   IOC now resolves optional parameters that are not registered with its provided value

### Fixed

-   Fixed problem with calculating index from older version value in specific case

## 12.2.0.0

### Added

-   new method DeleteAllData() on ObjectDBTransaction
-   PersistedNameAttribute is additionally allowed on interfaces - useful for Relations

## 12.1.0.0

### Added

-   Event deserialization now automatically converts Enums to integer types.

## 12.0.0.0

### Added

-   Changelog
-   Nullable support in both ODb and EventStore
