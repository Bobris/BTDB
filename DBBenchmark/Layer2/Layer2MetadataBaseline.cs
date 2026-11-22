using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using BTDB.Buffer;
using BTDB.EventStore2Layer;
using BTDB.EventStoreLayer;
using SimpleTester;

namespace DBBenchmark.Layer2;

/// <summary>
/// Phase 0 of BTDB.Replication.EventSourcing/Layer2MetadataFilePlan.md: what today's topic-style Layer2 metadata costs
/// at startup and in memory, and how large the metadata would be in the planned interned file format.
/// Run: dotnet run -c Release --project DBBenchmark -- layer2-metadata
/// </summary>
public static class Layer2MetadataBaseline
{
    const int Repeats = 5;

    sealed class NoTypesMapper(ITypeNameMapper inner) : ITypeNameMapper
    {
        public string ToName(Type type) => inner.ToName(type);
        // Events load as dynamic objects through the same descriptors: the release workload has several CLR types per
        // name, and loading into CLR types adds loader work that the planned change does not touch.
        public Type? ToType(string name) => null;
    }

    /// <summary>Release <paramref name="release"/> of an application whose event types are named
    /// <c>Shop.Events.Event0..</c>: it names top-level classes [release·names, (release+1)·names) after their event
    /// type and every other class by its full name, so each release adds one new version of every event type.</summary>
    sealed class ReleaseMapper(int release, int names) : ITypeNameMapper
    {
        const string Prefix = "Shop.Events.Event";
        readonly FullNameTypeMapper _inner = new();

        public string ToName(Type type) =>
            type.DeclaringType == null && type.Name.StartsWith("TestKlass", StringComparison.Ordinal) &&
            int.Parse(type.Name.AsSpan(9)) is var index && index / names == release
                ? Prefix + index % names
                : _inner.ToName(type);

        public Type? ToType(string name) =>
            name.StartsWith(Prefix, StringComparison.Ordinal)
                ? typeof(TestKlass0).Assembly.GetType("SimpleTester.TestKlass" +
                                                      (release * names + int.Parse(name.AsSpan(Prefix.Length))))
                : _inner.ToType(name);
    }

    sealed record Workload(string Name, ITypeNameMapper Mapper, object[] Instances, List<byte[]> MetadataLogs,
        List<byte[]> Data, Layer2Metadata File);

    public static void Run()
    {
        Console.WriteLine($"Runtime {Environment.Version}, {RuntimeInfo()}");
        var instances = InstantiateALotOfClasses.Gen().ToArray();
        // Warm up JIT of all measured paths on a small prefix.
        Measure(Produce("warmup", new FullNameTypeMapper(), instances[..50]), print: false);
        Measure(Produce("many types: 1000 classes, one process", new FullNameTypeMapper(), instances), print: true);
        Measure(ProduceReleases(50, instances), print: true);
    }

    static string RuntimeInfo() =>
        $"{System.Runtime.InteropServices.RuntimeInformation.OSDescription}, " +
        $"{System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture}, server GC {System.Runtime.GCSettings.IsServerGC}";

    static Workload Produce(string name, ITypeNameMapper mapper, object[] instances)
    {
        var logs = new List<byte[]>();
        var data = new List<byte[]>();
        SerializeAll(NewSerializer(mapper), instances, logs, data);
        var resolver = new Layer2TypeResolver(Layer2Metadata.Create("bench"));
        ResolveAll(resolver, mapper, instances);
        return new(name, mapper, instances, logs, data, resolver.Build());
    }

    // 1000/names releases, each a new process: it applies all metadata so far, then publishes one event of every
    // event type in its own version.
    static Workload ProduceReleases(int names, object[] all)
    {
        var logs = new List<byte[]>();
        var data = new List<byte[]>();
        var releases = all.Length / names;
        var file = Layer2Metadata.Create("bench");
        ITypeNameMapper mapper = null!;
        object[] current = [];
        for (var release = 0; release < releases; release++)
        {
            mapper = new ReleaseMapper(release, names);
            var serializer = NewSerializer(mapper);
            Apply(logs, serializer, null);
            current = all[(release * names)..((release + 1) * names)];
            SerializeAll(serializer, current, logs, data);
            var resolver = new Layer2TypeResolver(file);
            ResolveAll(resolver, mapper, current);
            file = resolver.Build();
        }
        return new($"releases: {names} event types x {releases} versions, measured as the last release", mapper,
            current, logs, data, file);
    }

    // The planned file: the type of every event resolved, as a catalog serializer would on its first use.
    static void ResolveAll(Layer2TypeResolver resolver, ITypeNameMapper mapper, object[] instances)
    {
        var creator = new EventSerializer(mapper);
        foreach (var instance in instances) resolver.Resolve(creator.Create(instance.GetType())!);
    }

    // Exactly like EventPublisher: new metadata is recorded as one log, applied back, and the event serialized again.
    static void SerializeAll(EventSerializer serializer, object[] instances, List<byte[]> logs, List<byte[]> data)
    {
        foreach (var instance in instances)
        {
            while (true)
            {
                var bytes = serializer.Serialize(out var metadata, instance).AsSyncReadOnlySpan().ToArray();
                if (!metadata)
                {
                    data.Add(bytes);
                    break;
                }
                logs.Add(bytes);
                serializer.ProcessMetadataLog(ByteBuffer.NewAsync(bytes));
            }
        }
    }

    static EventSerializer NewSerializer(ITypeNameMapper mapper) =>
        new(mapper, useInputDescriptors: false, forbidSerializationOfLazyDBObjects: true);

    static EventDeserializer NewDeserializer(ITypeNameMapper mapper) => new(new NoTypesMapper(mapper));

    static void Apply(IEnumerable<byte[]> logs, EventSerializer? serializer, EventDeserializer? deserializer)
    {
        foreach (var log in logs)
        {
            var buffer = ByteBuffer.NewAsync(log);
            serializer?.ProcessMetadataLog(buffer);
            deserializer?.ProcessMetadataLog(buffer);
        }
    }

    static void Measure(Workload w, bool print)
    {
        var instances = w.Instances;
        var probe = NewDeserializer(w.Mapper);
        Apply(w.MetadataLogs, null, probe);
        var descriptors = Descriptors(probe);
        var stats = SizeStats(descriptors, RootTypeIds(w.Data));

        // Startup: apply the whole metadata to a fresh serializer and deserializer, as EventPublisher does.
        var startup = Median(() =>
        {
            var s = NewSerializer(w.Mapper);
            var d = NewDeserializer(w.Mapper);
            var sw = Stopwatch.StartNew();
            Apply(w.MetadataLogs, s, d);
            return sw.Elapsed.TotalMilliseconds;
        });
        var prefixes = new[] { 0.25, 0.5, 0.75, 1.0 }.Select(fraction =>
        {
            var count = (int)Math.Round(w.MetadataLogs.Count * fraction);
            return (fraction, Median(() =>
            {
                var s = NewSerializer(w.Mapper);
                var d = NewDeserializer(w.Mapper);
                var sw = Stopwatch.StartNew();
                Apply(w.MetadataLogs.Take(count), s, d);
                return sw.Elapsed.TotalMilliseconds;
            }));
        }).ToArray();

        var serializerRetained = Retained(() =>
        {
            var s = NewSerializer(w.Mapper);
            Apply(w.MetadataLogs, s, null);
            return s;
        });
        var deserializerRetained = Retained(() =>
        {
            var d = NewDeserializer(w.Mapper);
            Apply(w.MetadataLogs, null, d);
            return d;
        });

        // First serialization of every type after a restart: metadata is complete, so only matching and savers.
        var firstSerialize = Median(() =>
        {
            var s = NewSerializer(w.Mapper);
            Apply(w.MetadataLogs, s, null);
            var sw = Stopwatch.StartNew();
            foreach (var instance in instances)
            {
                s.Serialize(out var metadata, instance);
                if (metadata) throw new InvalidOperationException("Complete metadata must not produce more.");
            }
            return sw.Elapsed.TotalMilliseconds;
        });
        var serializerAfterUse = Retained(() =>
        {
            var s = NewSerializer(w.Mapper);
            Apply(w.MetadataLogs, s, null);
            foreach (var instance in instances) s.Serialize(out _, instance);
            return s;
        });

        // First deserialization of every event after a restart: descriptors exist, loaders are generated.
        var firstDeserialize = Median(() =>
        {
            var d = NewDeserializer(w.Mapper);
            Apply(w.MetadataLogs, null, d);
            var sw = Stopwatch.StartNew();
            foreach (var data in w.Data)
                if (!d.Deserialize(out _, ByteBuffer.NewAsync(data)))
                    throw new InvalidOperationException("Metadata missing.");
            return sw.Elapsed.TotalMilliseconds;
        });
        var deserializerAfterUse = Retained(() =>
        {
            var d = NewDeserializer(w.Mapper);
            Apply(w.MetadataLogs, null, d);
            foreach (var data in w.Data) d.Deserialize(out _, ByteBuffer.NewAsync(data));
            return d;
        });

        // Spike cost: every class field gets its own structural descriptors (lists, dictionaries), as the inline
        // format would build them. The clones belong to a twin deserializer allocated outside the measurement: a
        // clone with its owner's callbacks and unchanged items is the original itself. Descriptors use the callbacks
        // only for the type mapper and converters, which the twin shares.
        var twin = NewDeserializer(w.Mapper);
        var unshared = default(UnsharedStats);
        var sharedAfterUse = deserializerAfterUse;
        var unsharedApplied = Retained(() =>
        {
            var d = NewDeserializer(w.Mapper);
            Apply(w.MetadataLogs, null, d);
            unshared = Unshare(d, twin);
            return d;
        });
        var unsharedAfterUse = Retained(() =>
        {
            var d = NewDeserializer(w.Mapper);
            Apply(w.MetadataLogs, null, d);
            Unshare(d, twin);
            foreach (var data in w.Data)
                if (!d.Deserialize(out _, ByteBuffer.NewAsync(data)))
                    throw new InvalidOperationException("Metadata missing.");
            return d;
        });
        GC.KeepAlive(twin);

        // The implemented format (Phase 2): load, resolve every event type after a restart, decode every type.
        var fileBytes = w.File.Bytes.ToArray();
        var load = Median(() =>
        {
            var sw = Stopwatch.StartNew();
            Layer2Metadata.Load(fileBytes.ToArray());
            return sw.Elapsed.TotalMilliseconds;
        });
        var loaded = Layer2Metadata.Load(fileBytes.ToArray());
        var resolveAll = Median(() =>
        {
            var resolver = new Layer2TypeResolver(loaded);
            var sw = Stopwatch.StartNew();
            ResolveAll(resolver, w.Mapper, instances);
            var elapsed = sw.Elapsed.TotalMilliseconds;
            if (resolver.HasNewTypes) throw new InvalidOperationException("The file must hold every type.");
            return elapsed;
        });
        var fileRetained = Retained(() => Layer2Metadata.Load(fileBytes.ToArray()));
        var decodeAll = Median(() =>
        {
            var decoder = new Layer2DescriptorDecoder(NewDeserializer(w.Mapper));
            var sw = Stopwatch.StartNew();
            for (var id = Layer2Metadata.FirstTypeId; loaded.ContainsType(id); id++) decoder.Get(loaded, id);
            return sw.Elapsed.TotalMilliseconds;
        });
        var twin2 = NewDeserializer(w.Mapper);
        var decodedRetained = Retained(() =>
        {
            var decoder = new Layer2DescriptorDecoder(twin2);
            for (var id = Layer2Metadata.FirstTypeId; loaded.ContainsType(id); id++) decoder.Get(loaded, id);
            return decoder;
        });
        GC.KeepAlive(twin2);

        // Phase 3: a restarted catalog writer serializes every measured event once; a fresh catalog reader reads them.
        CatalogEventSerializer NewCatalogSerializer() =>
            new(loaded, w.Mapper, forbidSerializationOfLazyDBObjects: true);
        var catalogFirstSerialize = Median(() =>
        {
            var serializer = NewCatalogSerializer();
            var sw = Stopwatch.StartNew();
            foreach (var instance in instances)
            {
                serializer.Serialize(out var proposed, instance);
                if (proposed != null) throw new InvalidOperationException("Complete metadata must not grow.");
            }
            return sw.Elapsed.TotalMilliseconds;
        });
        var catalogSerializerAfterUse = Retained(() =>
        {
            var serializer = NewCatalogSerializer();
            foreach (var instance in instances) serializer.Serialize(out _, instance);
            return serializer;
        });
        var catalogData = new List<byte[]>();
        var dataWriter = NewCatalogSerializer();
        foreach (var instance in instances)
            catalogData.Add(dataWriter.Serialize(out _, instance).AsSyncReadOnlySpan().ToArray());
        var catalogFirstDeserialize = Median(() =>
        {
            var deserializer = new CatalogEventDeserializer(loaded, new NoTypesMapper(w.Mapper));
            var sw = Stopwatch.StartNew();
            foreach (var data in catalogData)
                if (!deserializer.Deserialize(out _, ByteBuffer.NewAsync(data)))
                    throw new InvalidOperationException("Metadata missing.");
            return sw.Elapsed.TotalMilliseconds;
        });
        var catalogDeserializerAfterUse = Retained(() =>
        {
            var deserializer = new CatalogEventDeserializer(loaded, new NoTypesMapper(w.Mapper));
            foreach (var data in catalogData) deserializer.Deserialize(out _, ByteBuffer.NewAsync(data));
            return deserializer;
        });

        if (!print) return;
        var t = descriptors.Count;
        Console.WriteLine();
        Console.WriteLine($"== {w.Name}");
        Console.WriteLine($"user types T                         {t,10}");
        Console.WriteLine($"  of them classes / lists / dicts    {stats.Classes,10} / {stats.Lists} / {stats.Dictionaries}");
        Console.WriteLine($"metadata logs                        {w.MetadataLogs.Count,10}");
        Console.WriteLine($"metadata bytes, today                {w.MetadataLogs.Sum(l => l.Length),10}");
        Console.WriteLine($"  name bytes (UTF-8, every use)      {stats.NameBytes,10}   in {stats.NameUses} uses");
        Console.WriteLine($"  distinct names S / their bytes     {stats.DistinctNames,10} / {stats.DistinctNameBytes}");
        Console.WriteLine($"projected file bytes, as planned     {stats.PlannedFile,10}");
        Console.WriteLine($"  header + 4*S + 12*T                {24 + 4L * stats.DistinctNames + 12L * t,10}");
        Console.WriteLine($"  string bytes                       {stats.DistinctNameBytes,10}");
        Console.WriteLine($"  record bytes                       {stats.RecordBytes,10}");
        Console.WriteLine($"spike: structural types inline       {stats.InlineFile,10}");
        Console.WriteLine($"  types with an entry T'             {stats.InlineTypes,10}   ({stats.StandaloneStructural} structural used by ID in data)");
        Console.WriteLine($"  header + 4*S + 12*T'               {24 + 4L * stats.DistinctNames + 12L * stats.InlineTypes,10}");
        Console.WriteLine($"  record bytes                       {stats.InlineRecordBytes,10}");
        Console.WriteLine($"implemented file bytes               {w.File.Bytes.Length,10}   ({w.File.TypeCount} types, {w.File.StringCount} strings)");
        Console.WriteLine($"  load + checksum (median ms)        {load,10:F2}");
        Console.WriteLine($"  resolve every event type (ms)      {resolveAll,10:F1}   (fresh resolver on the loaded file)");
        Console.WriteLine($"  retained by the loaded file        {fileRetained,10} B  = {fileRetained / (double)t,6:F0} B/type of today");
        Console.WriteLine($"  decode every type (median ms)      {decodeAll,10:F1}");
        Console.WriteLine($"  retained by all decoded types      {decodedRetained,10} B  (today's deserializer after apply: {deserializerRetained} B)");
        Console.WriteLine($"catalog, {instances.Length} events, after a restart");
        Console.WriteLine($"  first serialize of all (median ms) {catalogFirstSerialize,10:F1}   (today {firstSerialize:F1}, after applying the log)");
        Console.WriteLine($"  serializer retained after          {catalogSerializerAfterUse,10} B  (today {serializerAfterUse} B)");
        Console.WriteLine($"  first deserialize (median ms)      {catalogFirstDeserialize,10:F1}   (today {firstDeserialize:F1} for {w.Data.Count} events)");
        Console.WriteLine($"  deserializer retained after        {catalogDeserializerAfterUse,10} B  (today {deserializerAfterUse} B for {w.Data.Count} events)");
        Console.WriteLine($"startup apply, ser+deser (median ms) {startup,10:F1}");
        foreach (var (fraction, ms) in prefixes)
            Console.WriteLine($"  first {fraction,4:P0} of logs              {ms,10:F1}");
        Console.WriteLine($"retained after apply: serializer     {serializerRetained,10} B  = {serializerRetained / (double)t,6:F0} B/type");
        Console.WriteLine($"retained after apply: deserializer   {deserializerRetained,10} B  = {deserializerRetained / (double)t,6:F0} B/type");
        Console.WriteLine($"first serialize of all (median ms)   {firstSerialize,10:F1}");
        Console.WriteLine($"  serializer retained after          {serializerAfterUse,10} B");
        Console.WriteLine($"first deserialize of all (median ms) {firstDeserialize,10:F1}");
        Console.WriteLine($"  deserializer retained after        {deserializerAfterUse,10} B");
        var structural = stats.Lists + stats.Dictionaries;
        var copiesBytes = unsharedApplied - deserializerRetained;
        Console.WriteLine($"spike: unshared structural descriptors in the deserializer");
        Console.WriteLine($"  shared today (lists + dicts)       {structural,10}");
        Console.WriteLine($"  unshared copies (one per use)      {unshared.Copies,10}   ({unshared.Lists} lists, {unshared.Dictionaries} dicts)");
        Console.WriteLine($"  copies retained after apply        {copiesBytes,10} B  = {copiesBytes / (double)unshared.Copies,6:F0} B/copy");
        Console.WriteLine($"    of it Name strings (estimate)    {unshared.NameBytes,10} B  = {unshared.NameBytes / (double)unshared.Copies,6:F0} B/copy");
        Console.WriteLine($"  allocated while copying            {unshared.Allocated,10} B  = {unshared.Allocated / (double)unshared.Copies,6:F0} B/copy");
        Console.WriteLine($"  extra after deserializing all      {unsharedAfterUse - sharedAfterUse,10} B");
        var inlineEstimate = deserializerRetained + copiesBytes - structural * (copiesBytes / (double)unshared.Copies);
        Console.WriteLine($"  deserializer with all built, est.  {inlineEstimate,10:F0} B  (today {deserializerRetained} B)");
    }

    sealed record UnsharedStats(int Copies, int Lists, int Dictionaries, long NameBytes, long Allocated);

    // Replaces every structural type referenced from a class field by a fresh copy, recursively; named types stay
    // shared. Returns how many copies were made and the estimated size of their Name strings.
    static UnsharedStats Unshare(EventDeserializer deserializer, EventDeserializer twin)
    {
        int copies = 0, lists = 0, dictionaries = 0;
        long nameBytes = 0;

        // One delegate for all calls, so allocations count only the copies and their names.
        Func<ITypeDescriptor, ITypeDescriptor> map = null!;
        map = d =>
        {
            if (!IsStructural(d)) return d; // classes, enums, built-ins
            var copy = d.CloneAndMapNestedTypes(twin, map);
            copies++;
            if (copy.GetType().Name.StartsWith("List", StringComparison.Ordinal)) lists++;
            else if (copy.GetType().Name.StartsWith("Dictionary", StringComparison.Ordinal)) dictionaries++;
            // .NET string: 20 B header and length plus 2 B per char, rounded up to 8.
            nameBytes += (20 + 2L * (copy.Name?.Length ?? 0) + 7) / 8 * 8;
            return copy;
        };

        var classes = Descriptors(deserializer).Values.OfType<ObjectTypeDescriptor>().ToList();
        var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        foreach (var o in classes) o.MapNestedTypes(map);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
        return new(copies, lists, dictionaries, nameBytes, allocated);
    }

    static double Median(Func<double> run)
    {
        var results = new double[Repeats];
        for (var i = 0; i < Repeats; i++) results[i] = run();
        Array.Sort(results);
        return results[Repeats / 2];
    }

    // Live heap growth while build's result is kept. GC.GetTotalMemory reported exactly twice the live bytes on
    // .NET 10.0.15 macOS arm64 (a retained byte[96], 120 B, showed as 240 B); the heap size minus fragmentation
    // after a full blocking collection measures 120 B.
    static long Retained(Func<object> build)
    {
        var before = LiveHeap();
        var kept = build();
        var after = LiveHeap();
        GC.KeepAlive(kept);
        return after - before;
    }

    static long LiveHeap()
    {
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        var info = GC.GetGCMemoryInfo(GCKind.FullBlocking);
        return info.HeapSizeBytes - info.FragmentedBytes;
    }

    // User descriptors (ID >= 50) by ID, read from the deserializer's private table; measurement only.
    static SortedDictionary<int, ITypeDescriptor> Descriptors(EventDeserializer deserializer)
    {
        var table = (IDictionary)typeof(EventDeserializer)
            .GetField("_typeOrDescriptor2Info", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(deserializer)!;
        var result = new SortedDictionary<int, ITypeDescriptor>();
        foreach (var info in table.Values)
        {
            var type = info!.GetType();
            var id = (int)type.GetField("Id")!.GetValue(info)!;
            if (id < EventDeserializer.ReservedBuildinTypes) continue;
            result[id] = (ITypeDescriptor)type.GetField("Descriptor")!.GetValue(info)!;
        }
        return result;
    }

    sealed record Stats(int Classes, int Lists, int Dictionaries, long NameBytes, int NameUses, int DistinctNames,
        long DistinctNameBytes, long RecordBytes, long PlannedFile, int InlineTypes, int StandaloneStructural,
        long InlineRecordBytes, long InlineFile);

    // Type IDs the data stream stores by ID at the top level; nested objects are not inspected, the workload has no
    // object-typed fields, so only roots can name a structural type by ID.
    static HashSet<int> RootTypeIds(List<byte[]> data)
    {
        var ids = new HashSet<int>();
        foreach (var bytes in data) ids.Add((int)PackUnpack.UnpackVUInt(bytes));
        return ids;
    }

    static bool IsStructural(ITypeDescriptor d) => d.GetType().Name is "ListTypeDescriptor" or
        "DictionaryTypeDescriptor" or "NullableTypeDescriptor" or "TupleTypeDescriptor";

    static bool IsNamed(ITypeDescriptor d) => d is ObjectTypeDescriptor || d.GetType().Name == "EnumTypeDescriptor";

    static List<ITypeDescriptor> Nested(ITypeDescriptor d)
    {
        var nested = new List<ITypeDescriptor>();
        d.MapNestedTypes(n =>
        {
            nested.Add(n);
            return n;
        });
        return nested;
    }

    // Projects the planned format (plan section 4) from the descriptors: names interned without a stored string
    // index, references as varints. The spike also writes structural types (list, dictionary, nullable, tuple) inside
    // the record that refers to them; only named types and structural types the data stores by ID keep an entry.
    // An inline reference is a varint: 0–3 tag an inline structural type, any other value is type ID + 4.
    static Stats SizeStats(SortedDictionary<int, ITypeDescriptor> descriptors, HashSet<int> dataTypeIds)
    {
        var ids = new Dictionary<ITypeDescriptor, int>(ReferenceEqualityComparer.Instance);
        foreach (var (id, d) in descriptors) ids[d] = id;
        var strings = new Dictionary<string, int>(StringComparer.Ordinal) { ["binding-name"] = 0 };
        long nameBytes = 0, recordBytes = 0, inlineRecordBytes = 0;
        int nameUses = 0, classes = 0, lists = 0, dictionaries = 0;

        int StringId(string s, bool count)
        {
            if (count)
            {
                nameBytes += Encoding.UTF8.GetByteCount(s);
                nameUses++;
            }
            if (!strings.TryGetValue(s, out var id)) strings.Add(s, id = strings.Count);
            return id;
        }

        int RefId(ITypeDescriptor d) => ids.TryGetValue(d, out var id) ? id : 2; // built-in: a small ID, one byte

        // Spike numbering: named types and structural types used by ID in data, in their original order.
        var inlineIds = new Dictionary<ITypeDescriptor, int>(ReferenceEqualityComparer.Instance);
        var standalone = 0;
        foreach (var (id, d) in descriptors)
        {
            var keep = IsNamed(d) || dataTypeIds.Contains(id);
            if (!keep) continue;
            if (!IsNamed(d)) standalone++;
            inlineIds[d] = EventDeserializer.ReservedBuildinTypes + inlineIds.Count;
        }

        long InlineRef(ITypeDescriptor d)
        {
            if (inlineIds.TryGetValue(d, out var id)) return VarSize(id + 4);
            if (!ids.ContainsKey(d)) return 1; // built-in
            var size = 1L; // tag
            var nested = Nested(d);
            if (d.GetType().Name.StartsWith("Tuple", StringComparison.Ordinal)) size += VarSize(nested.Count);
            foreach (var n in nested) size += InlineRef(n);
            return size;
        }

        long Body(ITypeDescriptor d, bool count, Func<ITypeDescriptor, long> reference)
        {
            long size = 1; // category
            if (d is ObjectTypeDescriptor o)
            {
                size += VarSize(StringId(o.Name, count));
                var fields = o.Fields.ToList();
                size += VarSize(fields.Count);
                foreach (var (fieldName, fieldType) in fields)
                    size += VarSize(StringId(fieldName, count)) + reference(fieldType);
                return size;
            }
            var nested = Nested(d);
            if (d.GetType().Name.StartsWith("Tuple", StringComparison.Ordinal)) size += VarSize(nested.Count);
            foreach (var n in nested) size += reference(n);
            return size;
        }

        foreach (var (id, d) in descriptors)
        {
            if (d is ObjectTypeDescriptor) classes++;
            else if (d.GetType().Name.StartsWith("List", StringComparison.Ordinal)) lists++;
            else if (d.GetType().Name.StartsWith("Dictionary", StringComparison.Ordinal)) dictionaries++;
            else throw new NotSupportedException("Unexpected descriptor " + d.GetType().Name);
            recordBytes += Body(d, true, n => VarSize(RefId(n)));
            if (inlineIds.ContainsKey(d)) inlineRecordBytes += Body(d, false, InlineRef);
        }

        var distinctBytes = strings.Keys.Sum(s => (long)Encoding.UTF8.GetByteCount(s));
        var t = descriptors.Count;
        var file = 24 + 4L * strings.Count + 12L * t + distinctBytes + recordBytes;
        var inlineFile = 24 + 4L * strings.Count + 12L * inlineIds.Count + distinctBytes + inlineRecordBytes;
        return new(classes, lists, dictionaries, nameBytes, nameUses, strings.Count, distinctBytes, recordBytes, file,
            inlineIds.Count, standalone, inlineRecordBytes, inlineFile);
    }

    static int VarSize(long value)
    {
        var size = 1;
        while (value >= 0x80)
        {
            value >>= 7;
            size++;
        }
        return size;
    }
}
