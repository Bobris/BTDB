# Layer2 metadata file — plan

Status: implemented. Phase 0 (measurement tool `DBBenchmark -- layer2-metadata`, results in section 9), Phase 1
(`IVersionedBlob` and its implementations, section 3), Phase 2 (the file format, resolver and decoder in
`BTDB/EventStore2Layer`, sections 4–6), Phase 3 (`CatalogEventSerializer` and `CatalogEventDeserializer`, section 10),
Phase 4 (`EventPublisher` on the metadata file, section 7) and Phase 5 (docs) are done. Not done: measuring
new-type publication latency on live Azure, and the later port of the topic API onto the catalog (decision 1). The package is not deployed anywhere, so the metadata topic is
replaced without migration or backward compatibility.

## 1. Goal

Replace the Layer2 metadata EventLog topic of `EventPublisher` with one versioned blob that is read whole and written
whole with an ETag condition, and keep the metadata in memory close to its serialized form instead of two parsed
descriptor graphs.

What it replaces today:

- Publishing: `EventSerializer.Serialize` reports new metadata, the publisher appends it to the metadata topic, waits
  until its own topic reader applies it, serializes again and publishes the data (`EventPublisher.PublishStoredAsync`).
- Startup: the whole metadata topic is read from offset 0 and followed live; every record is applied to both
  `EventSerializer` and `EventDeserializer`.
- Memory: each of the two instances keeps a full `ITypeDescriptor` graph of every type ever stored, including old
  versions it never uses; every `SerializerTypeInfo` allocates two dictionaries; both deduplicate with structural
  `Equals` against all known types, O(N²) when a large log is applied at once.

## 2. Decisions

| # | Decision |
|---|----------|
| 1 | The existing `EventSerializer`/`EventDeserializer` topic API (`ProcessMetadataLog`) stays exactly as it is, including its ID assignment. The catalog is a second path used by EventSourcing. Porting the old API onto the catalog is later work and needs a test proving identical ID assignment on existing logs. |
| 2 | Recursive types get position-independent shape keys per strongly connected group (section 5). Accepting duplicates is ruled out: a cyclic type would never match and every new process would append another copy. |
| 3 | A new minimal `IVersionedBlob` abstraction (section 3), not `IEventLogStorage`. The local implementation keeps its version only in memory. |
| 4 | Refresh reads the whole file. Writes are whole anyway. |
| 5 | Shape keys are stored in the file; every index hit is verified by comparing bytes. |
| 6 | Strings (type, field and enum member names) are interned in one string table; records refer to string IDs. |
| 7 | Record and string offsets are stored in the file; loading needs no parsing. |
| 8 | One checksum covers the whole file. |
| 9 | Shape keys are u32 and the key index is stored sorted in the file, not built at load (section 4). There is no string index: shape keys hash name bytes, not string IDs (section 5; Phase 0 evidence in section 9). |
| 10 | Structural types (list, dictionary, nullable, tuple) are written inside the record that refers to them; only named types (class, enum) and structural types that data stores by ID get an entry (section 4). The deserializer deduplicates structural descriptors while building them, so they stay shared as today (section 6). Phase 0 evidence in section 9: the file shrinks 14%; unshared descriptors would cost about 200 B per use. |

## 3. `IVersionedBlob`

One blob per instance; versions are opaque strings.

```csharp
public sealed record VersionedBlobContent(string Version, ReadOnlyMemory<byte> Content);
public enum VersionedBlobWriteOutcome { Applied, Rejected, Ambiguous }
public readonly record struct VersionedBlobWriteResult(VersionedBlobWriteOutcome Outcome, string? Version = null);

public interface IVersionedBlob
{
    /// <summary>Null when the blob does not exist. Throws on transport errors.</summary>
    ValueTask<VersionedBlobContent?> ReadAsync(CancellationToken cancellation);

    /// <summary>Replace when the current version equals expectedVersion, or create when expectedVersion is null and
    /// the blob does not exist. Never retried transparently: a possibly landed write is Ambiguous.</summary>
    ValueTask<VersionedBlobWriteResult> WriteAsync(string? expectedVersion, ReadOnlyMemory<byte> content,
        CancellationToken cancellation);
}
```

Implemented in `BTDB.Replication/IVersionedBlob.cs` with the interface above, plus static
`VersionedBlobWriteResult.Rejected`/`Ambiguous`. Implementations:

- `InMemoryVersionedBlob` (`BTDB.Replication`, public, for tests of one process): the internal `NextWriteFault`
  rejects, or reports Ambiguous without or after applying, the next write. It was internal until Phase 4 needed it in
  three test hosts without internals access (BackendActions tests, the AOT smoke program).
- `FileSystemVersionedBlob(path, flushToDisk)` (`BTDB.Replication`): one instance owns the file, like
  `FileSystemObjectStore` owns its directory.
  - The version exists only in memory: a fresh random value when the instance opens an existing file and after every
    write, null while the file is missing. It is never stored. Only writers inside this process can hold a version,
    and they all go through the same instance, so a version from an earlier process can never be presented.
  - An exclusive `.lock` file next to the target, held for the instance's lifetime, rejects a second instance over
    the same path, in this process or another. Two instances would each keep their own in-memory version and lose
    each other's updates. A restarted publisher disposes the old instance first.
  - A write goes to `{path}.tmp`, is flushed and is atomically moved over the target. Rewriting in place could
    leave a torn file after a crash, and the checksum would then only detect the loss of all metadata. Opening
    deletes a leftover `{path}.tmp`.
  - Local writes are never Ambiguous: they either apply or throw.
- `AzureVersionedBlob(BlobClient)` (`BTDB.Replication.Azure`): Block Blob, `If-Match` / `If-None-Match: *`, client
  with `Retry.MaxRetries = 0`; 404/409/412 are Rejected, transient failures are Ambiguous (same classification as
  `AzureEventLogStorage`). A missing container throws `InvalidOperationException`: it would otherwise read as a
  missing blob whose every create is rejected, a livelock.

Tests: `BTDB.Replication.Test/VersionedBlobContract.cs` (shared by all three: create, conflicts, copies, empty and
3 MiB content, cancellation), `VersionedBlobTest` (faults, a second file instance, reopen, crash leftovers, disposal)
and `BTDB.Replication.Azure.Test/AzureVersionedBlobTest` (contract on Azurite, lost upload response, missing
container).

## 4. File format

All fixed-width fields are little-endian. Type ID = 50 + type index (0 and 1 are null and back reference, 2–49 are
built-in types). String ID = string index. Type IDs are the ones data uses, unchanged.

| Section | Content |
|---------|---------|
| Header (24 B) | magic `BL2M` (u32), format version 1 (u32), XxHash64 of bytes 16..end (u64), string count S (u32), type count T (u32) |
| String ends | u32[S]: end offset of each string in the string bytes; a string starts at the previous end |
| Record ends | u32[T]: end offset of each record in the record bytes |
| Key index | (u32 shape key, u32 type index)[T], sorted by key (section 5) |
| String bytes | UTF-8 of all strings, concatenated, each string exactly once |
| Record bytes | all records, concatenated |

String 0 is the binding name: the data topic name. Opening a file bound to another topic fails, which keeps the rule
that metadata is never shared with an unrelated data stream.

Records use varints. A name is a string ID. A type reference is a varint `r`:

| `r` | Meaning |
|-----|---------|
| 0 | inline list: item type reference follows |
| 1 | inline dictionary: key and value type references follow |
| 2 | inline nullable: item type reference follows |
| 3 | inline tuple: count, then item type references |
| ≥ 4 | type ID `r − 4`: built-in or an entry |

Types up to ID 123 still take one byte. Structural types are written inline because data writes them inline too:
only class instances, and values at the event root or in an `object`-typed position, carry a type ID in data
(`ObjectTypeDescriptor.StoredInline` is false, every structural descriptor's is true).

Entries and their records:

| Category (`TypeCategory` byte) | Payload |
|-----|---------|
| Class | name, field count, (field name, type reference)* |
| Enum | name, header (signed \| flags << 1 \| count << 2), (member name, value)* |
| List, Dictionary, Nullable, Tuple | the inline payload of that category, written only for a structural type that data stores by ID; created on demand |

A writer only appends: new strings after old strings, new records after old records. So a refresh checks that the
new file's string bytes and record bytes start with the old ones (two `SequenceEqual`s) and that the old ends are
unchanged. The key index is rebuilt by every write (cheap, the write is whole anyway) and is not part of that check.

Load validation: magic, version, checksum, total length = 24 + 4·(S + 3T) + string bytes + record bytes, ends are
non-decreasing, strings are valid UTF-8, the key index is sorted and in range. A damaged index entry can only cause a
missed lookup, so a duplicate append, never a wrong ID, because every hit is verified. Writers never store a string
twice, but a duplicate would be harmless (keys and verification use name bytes), so load does not check uniqueness.
Type references and string IDs inside a record are checked when it is decoded; a reference to a later type is allowed
(cycles).

Why the key index is stored: the file stays in memory anyway. Stored sorted, it costs 8 B per type in total with
nothing built at load; built at load it would cost about 8 B per type of hash table on top of 4 B per type of keys in
the file, plus an O(N) build on every node. Lookups are binary searches (about 14 steps for 10,000 types), done once
per CLR type per process.

Why there is no string index: only a writer appending new types needs name → string ID, and it builds a temporary map
from the string table for that write. Lookups never need it because shape keys hash name bytes (section 5). Dropping it
saves 4 B per string, about 10% of the file in the Phase 0 workloads.

Shape keys are u32: every hit is verified by bytes, so a collision costs one comparison and never a wrong ID.

## 5. Shape keys

The canonical encoding of a type is its record with every name written as its UTF-8 bytes (length-prefixed) instead
of its string ID, so the serializer computes it from a CLR type without looking up strings. Inline structural types
are part of the referring record's encoding and have no key of their own; only entries have keys. Verifying a hit
decodes the stored record and compares names through the string table.

Keys are computed bottom-up over the entries reachable from a CLR type, one strongly connected group at a time,
children first:

- Non-cyclic type: children already have IDs. Key = low 32 bits of XxHash64(canonical encoding).
- Cyclic group of k types: order the members by their encodings with in-group references masked (category, then
  name, then the rest); encode each member with in-group references as local indexes. Group hash G = XxHash64 of the
  length-prefixed member encodings; member key i = low 32 bits of XxHash64(G as 8 little-endian bytes, seed i). New
  groups take consecutive IDs in that order, so their records are deterministic and a stored group is verified from
  the candidates of member 0's key: candidate c matches when records c … c + k − 1 encode as the members.
- References in canonical encodings are varints: 0–3 inline structural types as in records, 4 an in-group
  reference followed by its local index, 5 a masked in-group reference, any other value a type ID + 6.
- Ties in the member order cannot happen for descriptors built from CLR types (one descriptor per type name). If one
  does, fall back to discovery order; the cost is a duplicate append, not a wrong ID.
- Groups contain only named types: `List<Node>` inside `Node` is a self-reference within one record.

## 6. In memory

Implemented in `BTDB/EventStore2Layer`: `Layer2Metadata` (public: `Create`, `Load`, `Bytes`, `BindingName`,
`Extends`, `ContainsType`), `Layer2MetadataBuilder`, `Layer2TypeResolver` and `Layer2DescriptorDecoder` (internal
until Phase 3 builds the public serializer and deserializer on them). The descriptors got internal constructors from
parts and accessors of their parts.

`Layer2Metadata`, an immutable snapshot shared by the serializer and deserializer:

- the file's `byte[]` as read, used as is: strings, records and the key index are read from it through the stored
  offsets; nothing else is allocated per type or per string.

Per-instance caches only for what an instance touches (descriptors are bound to their owning
`ITypeDescriptorCallbacks`, so they are not shared):

- serializer: CLR type → (type ID, saver), for classes, enums and structural types it stores by ID. A list or
  dictionary in a typed field needs no ID.
- deserializer: type ID → (descriptor, loader). A descriptor is built from its record together with every
  not-yet-built type it reaches (shells first, then the existing placeholder / `MapNestedTypes` passes for cycles).
  Old versions that are never read are never built.
- deserializer: structural descriptors are deduplicated while building, through a map keyed by the inline encoding
  (the bytes of the reference in the record, compared in place in the file buffer). Equal bytes mean equal shape,
  because named types are themselves deduplicated by ID. A standalone structural entry shares its descriptor with
  equal inline uses. Without it every use would hold its own descriptor and derived name string, about 200 B
  (section 9).

A refresh replaces the snapshot. Type and string IDs are stable, so the caches stay valid.

## 7. Flows

Publish (under the publisher's existing publishing semaphore):

1. Serialize. If every type is known, publish the data.
2. Otherwise resolve the new types (section 5), write old file + new strings + new records + keys with
   `expectedVersion` = current version. New strings get IDs from a temporary name → ID map built from the string
   table for this write.
   - Applied: install the new snapshot; the IDs are known, no readback. Go to 1.
   - Rejected or Ambiguous: read, install, go to 1. Lookup by shape makes this idempotent: types written by another
     node, or by this node's own landed ambiguous write, are found and nothing is appended again.

Startup: one read (a missing blob is an empty catalog with only the binding name), install, continue with data replay.
No live following of metadata.

Consume: an unknown type ID triggers one single-flight refresh. Metadata is durable before any data that uses it, so
an ID still missing after the refresh is corruption (`InvalidDataException`).

## 8. Phases

0. Baseline: memory per type, startup time with many types and new-type publish latency of today's implementation,
   using `DBBenchmark -- layer2-metadata` (section 9), repeated after Phase 4 for comparison.
1. `IVersionedBlob` with the in-memory, local-file and Azure implementations; tests for conflicts, ambiguous writes,
   a second local instance over the same path, and crash atomicity of the local file.
2. Done. `BTDB/EventStore2Layer`: file writer/reader, validation, sorted key index, shape keys with cyclic groups,
   descriptor building from records (the descriptors need internal constructors from parts), inline structural types
   and their deduplication. Tests (`BTDBTest/Layer2MetadataTest`): round trips of every category, nested inline types
   (`Dictionary<string, List<int>>`), a standalone structural type sharing its descriptor with equal inline uses,
   A↔B cycles giving the same file from either entry, `List<Node>` inside `Node`, versions of one name, appending,
   any changed byte rejected on load, damage with a valid checksum failing only with `InvalidDataException`
   (fuzz), a colliding key, and nothing left half built after a damaged record. Choosing standalone entries for
   `object`-typed fields and event roots belongs to the serializer (Phase 3).
3. Done. `CatalogEventSerializer`/`CatalogEventDeserializer` (section 10). Tests: a `Catalog` layer in
   `EventLayerRoundTripHelper` runs the existing event-layer theories through the catalog path (83 cases: every
   descriptor category, deep nesting, emit and no-emit modes, fuzz, encryption, renaming), and
   `BTDBTest/CatalogEventSerializerTest` covers one proposal for all new types of an event, a lost race, `object`-typed
   fields holding lists, enums and built-ins, subclasses, object cycles, a failed serialization leaving no proposal,
   loading an old version into the current type, `Update` validation and building only types that are read.
4. Done. `EventPublisher(database, dataTopic, IVersionedBlob metadata, …)`; `Layer2EventCodec` owns the file (read
   at startup, conditional writes, single-flight refresh on a missing type); the metadata topic reader and its waits
   are gone. Tests (`BTDB.Replication.EventSourcing.Test`, 71): data published only after its metadata write, two
   publishers racing for the first write (three write attempts, no type stored twice), rejected and ambiguous
   writes with and without landing, a consumer refreshing before dependent data, a restarted publisher writing
   nothing, damaged data and a file of another topic stopping the publisher, the 30-seed randomized multi-node test
   with jittered and failing metadata writes, and the Azure test with `AzureVersionedBlob` on Azurite. Callers
   updated: BackendActions tests and process test, EventLogBench, AotActionsSmoke.
5. Done. `CHANGELOG.md`, package README, `Doc/ReplicationEventSourcing.md`, `Doc/ReplicationHosting.md` (the port
   table), `BTDB.Replication/ObjectStorages.md`, `BTDB.Replication.Azure/README.md`, the main README.

## 9. Phase 0 results

`dotnet run -c Release --project DBBenchmark -- layer2-metadata` (`DBBenchmark/Layer2/Layer2MetadataBaseline.cs`),
.NET 10.0.15, macOS arm64, workstation GC, 2026-11-22. Workload: the 1000 generated classes of
`SimpleTester/ALotOfClasses.cs` (now marked `[Generate]`), one event per class. "Releases" names 50 event types and
publishes 20 versions of each, one release per fresh serializer. Times are medians of 5 runs.

| | many types | releases |
|---|---:|---:|
| user types T (classes / lists / dictionaries) | 4799 (2517 / 765 / 1517) | 5224 (2942 / 765 / 1517) |
| metadata bytes today (1000 logs) | 142,781 | 163,154 |
| name bytes in today's metadata, every use | 113,587 (80%) | 129,180 (79%) |
| distinct names S / their bytes | 5517 / 107,535 | 4992 / 94,110 |
| startup apply, serializer + deserializer | 222 ms | 277 ms |
| same for 25 / 50 / 75% of the logs | 12 / 53 / 129 ms | 14 / 65 / 155 ms |
| retained after apply: serializer / deserializer | 461 / 357 B per type | 462 / 357 B per type |
| serializer retained after first serialization of the measured events | 7.9 MB (1000 events) | 2.9 MB (50 events) |
| projected file, first draft with a string index | 243,253 | 236,699 |
| fixed tables in it, 24 + 8S + 12T | 101,748 | 102,648 |
| projected file, adopted (no string index) | 221,185 | 216,731 |
| record bytes in it | 33,970 | 39,941 |

Findings:

- Startup grows quadratically (4× the logs, about 18× the time): at 10× the types it would take tens of seconds.
- Today the metadata costs about 820 B per type in memory, two parsed graphs. The adopted file held as is would
  cost about 40–46 B per type, roughly 18–20× less.
- Retained memory is the live heap (heap size minus fragmentation) after a full blocking collection.
  `GC.GetTotalMemory` reported exactly twice the live bytes on this runtime (a retained 120 B `byte[96]` showed as
  240 B, reproduced in a standalone program); the first numbers taken with it were halved by this correction.
- In these generated classes names barely repeat (property names are unique per class), so interning saves only
  5% / 27% of name bytes. Real event models repeat field names (`Id`, `Name`, timestamps) far more; this workload is
  close to the worst case for interning.
- The fixed tables are larger than the records: a record averages about 7–8 B, its table entries 12 B, and a string
  entry cost 8 B in the first draft (4 B now) against an average string of about 19 B. With these names the first draft was 1.45–1.7× today's
  metadata, the adopted format is 1.33–1.55×. Projected variants (same tool):

| variant | many types | releases |
|---|---:|---:|
| with a stored string index (first draft) | 243,253 | 236,699 |
| no string index: shape keys hash name bytes, not string IDs, so the serializer needs no string lookup; a writer builds a temporary name → ID map only when appending — **adopted** | 221,185 | 216,731 |
| + packed widths: each table's entry width (1–4 B) chosen from its largest value and stored in the header | 196,472 | 190,843 |
| + u16 shape keys | 186,874 | 180,395 |

Packed widths and u16 keys are not adopted.

### Spike: structural types inside the record that refers to them

Lists, dictionaries, nullables and tuples are about half of all types in these workloads (2282 of 4799), yet each
costs 12 B of tables for a record of 2–3 B. Data already writes them inline: only class instances, and values in
`object`-typed positions or as the event root, carry a type ID (`ObjectTypeDescriptor.StoredInline` is false, every
structural descriptor's is true). So metadata can write them inline too:

- A type reference becomes a varint where 0–3 tag an inline list, dictionary, nullable or tuple (its payload follows)
  and any other value is type ID + 4. Types up to ID 123 still take one byte.
- Only named types (classes, enums) get an entry, plus a structural type when data needs it by ID (event root or an
  `object`-typed field); such a standalone entry is created on demand with the inline encoding as its record and key.
- Structural types get no shape key of their own; their shape is part of the referring record's canonical encoding.
  Cyclic groups contain only named types: `List<Node>` inside `Node` becomes a self-reference in one record.
- The deserializer builds a fresh structural descriptor per referring field, only for types it actually reads;
  loaders are generated per class as before.

Projected with the same tool (no string index in both):

| | many types | releases |
|---|---:|---:|
| types with an entry | 4799 → 2517 | 5224 → 2942 |
| structural types the data needs by ID | 0 | 0 |
| tables, 24 + 4S + 12T | 79,680 → 52,296 | 82,680 → 55,296 |
| record bytes | 33,970 → 30,099 | 39,941 → 37,984 |
| file | 221,185 → 189,930 (−14%) | 216,731 → 187,390 (−14%) |

Record bytes shrink here because these lists are rarely shared (each `LocalClass` has its own); with a widely shared
`List<string>` an inline copy costs 2 B per use against a 1–2 B reference, so records can grow slightly while the
tables still shrink.

Deserializer memory if every class field gets its own structural descriptors (same tool: each class field's lists
and dictionaries replaced by fresh copies, recursively, then all events deserialized):

| | many types | releases |
|---|---:|---:|
| structural descriptors, shared today | 2282 | 2282 |
| unshared copies, one per use | 3007 | 4279 |
| retained per copy | 203 B | 193 B |
| of it the derived `Name` string, e.g. `List<SimpleTester.TestKlass1+LocalClass0>` | 112 B | 112 B |
| allocated per copy | 193 B | 193 B |
| extra retained after deserializing all events | 580 KB | 824 KB |
| deserializer after apply, unshared vs today (estimate: today + copies − shared originals at the same size each) | 1.86 vs 1.71 MB (+9%) | 2.25 vs 1.87 MB (+21%) |

- A copy is the descriptor object (about 80 B) plus its own name string (112 B, 58%). Descriptors cache no loaders:
  the extra after deserializing equals the copies, so loaders cost the same shared or not.
- The catalog deserializer builds only types it reads, so the extra applies only to read types. Deduplicating
  structural descriptors while building them (a per-deserializer map keyed by the inline encoding) restores today's
  sharing.

Status: adopted with deduplication (decision 10).

### Phase 2 results

Same tool and machine, the implemented format: `Layer2TypeResolver` resolves the type of every event (per release in
the releases workload) into a real file; then a fresh resolver resolves them all again on the loaded file, and a
`Layer2DescriptorDecoder` builds every stored type.

| | many types | releases |
|---|---:|---:|
| implemented file bytes (projection) | 189,922 (189,930) | 187,382 (187,390) |
| types / strings | 2517 / 5517 | 2942 / 4992 |
| load including checksum, vs today's startup apply | 0.6 vs 226 ms | 0.1 vs 284 ms |
| retained by the loaded file | 190 KB | 188 KB |
| resolve every event type on the loaded file | 53 ms (1000 types) | 5 ms (50 types) |
| decode every stored type | 2.6 ms | 2.5 ms |
| retained by all decoded types, vs today's deserializer after apply | 1.43 vs 1.71 MB | 1.45 vs 1.87 MB |

The projection differs only by the binding name. Decoding everything is the worst case: a deserializer builds only
types it reads. The decoder also shares one string instance per string ID across descriptors.

### Phase 3 results

Same tool and machine: a restarted catalog writer on the loaded file serializes every measured event once (1000
events, or the last release's 50), and a fresh catalog reader reads them. Today's numbers come after applying the
whole metadata log (226 / 284 ms, not included in them).

| | many types | releases |
|---|---:|---:|
| first serialization of every event, catalog vs today | 53 vs 151 ms | 5 vs 24 ms |
| serializer retained after it, catalog vs today | 4.1 vs 7.9 MB | 0.4 vs 2.9 MB |
| first read of every event, catalog vs today | 8.2 vs 2.9 ms (1000 events) | 0.3 ms (50 events) vs 1.4 ms (1000 events) |
| deserializer retained after it, catalog vs today | 3.0 vs 3.3 MB (1000 events) | 0.4 MB (50 events) vs 3.9 MB (1000 events) |

The catalog reader is slower on first reads because it builds descriptors then; today builds them all while
applying the log at startup, which takes far longer.

## 10. API shape

- The catalog path has its own classes in `BTDB/EventStore2Layer`: `CatalogEventSerializer(metadata, mapper, …)` and
  `CatalogEventDeserializer(metadata, mapper, …)`. `_typeOrDescriptor2Info` is used throughout `EventSerializer`
  (`Create`, `StoreNewDescriptors`, `MergeDescriptor`, `StoreObject`), so a strategy inside it would touch every one of
  those places, against decision 1. The stable shared parts moved unchanged into the internal `Layer2Shared`: choosing
  the descriptor kind of a CLR type, building a saver and a loader, and encrypted strings; the topic classes call them.
- `CatalogEventSerializer.Serialize(out Layer2Metadata? newMetadata, object? obj)` returns the data, or an empty
  buffer and the installed metadata with every new type of the event appended. The caller writes it before any data
  that uses it, calls `Update` with the written or re-read metadata, and serializes again. Proposed IDs are never
  used before they are installed; IDs of types found in the installed metadata are kept even when other types of
  the same event were new, and a failed serialization drops its proposals.
- `CatalogEventDeserializer.Deserialize(out object? obj, ByteBuffer data)` returns false when the data uses a type
  the installed metadata lacks; the caller installs newer metadata with `Update` and tries again.
- Data has the format of `EventSerializer` with type IDs from the file. A field of type `object` (for example the item
  type of `List<object>`) refers to the class entry `System.Object`, as in metadata logs.
- `IVersionedBlob` and its implementations are public: applications construct them.
- The catalog and the catalog serializer/deserializer are public in BTDB core, usable without EventSourcing. The
  format carries a version number; until a release declares it stable, a format change may require recreating the
  file.
