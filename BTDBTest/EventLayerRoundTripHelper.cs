using System;
using System.Collections.Generic;
using BTDB.Encrypted;
using BTDB.EventStore2Layer;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

/// Type name mapper which allows renaming types and hiding types (making them unknown to reader).
public class RenamingTypeMapper : ITypeNameMapper
{
    readonly ITypeNameMapper _parent = new FullNameTypeMapper();
    readonly Dictionary<Type, string> _type2Name = new();
    readonly Dictionary<string, Type?> _name2Type = new();

    public RenamingTypeMapper Map(Type type, string name)
    {
        _type2Name[type] = name;
        _name2Type[name] = type;
        return this;
    }

    public RenamingTypeMapper Hide(string name)
    {
        _name2Type[name] = null;
        return this;
    }

    public string ToName(Type type)
    {
        return _type2Name.TryGetValue(type, out var name) ? name : _parent.ToName(type);
    }

    public Type? ToType(string name)
    {
        return _name2Type.TryGetValue(name, out var type) ? type : _parent.ToType(name);
    }
}

public enum EventLayer
{
    Mapping,
    Store1,
    Store2
}

public static class EventLayerRoundTripHelper
{
    public static object? RoundTrip(EventLayer layer, object? value, ITypeNameMapper? writeMapper = null,
        ITypeNameMapper? readMapper = null, ISymmetricCipher? cipher = null)
    {
        return layer switch
        {
            EventLayer.Mapping => RoundTripMapping(value, writeMapper, readMapper, CreateOptions(cipher),
                CreateOptions(cipher)),
            EventLayer.Store1 => RoundTrip1(value, writeMapper, readMapper, CreateOptions(cipher),
                CreateOptions(cipher)),
            EventLayer.Store2 => RoundTrip2(value, writeMapper, readMapper, cipher: cipher),
            _ => throw new ArgumentOutOfRangeException(nameof(layer))
        };
    }

    public static TypeSerializersOptions CreateOptions(ISymmetricCipher? cipher) => new()
    {
        IgnoreIIndirect = true, PreserveDescriptors = true, SymmetricCipher = cipher
    };

    /// Serializes using EventStore2Layer EventSerializer and deserializes by fresh EventDeserializer.
    public static object? RoundTrip2(object? value, ITypeNameMapper? writeMapper = null,
        ITypeNameMapper? readMapper = null, ITypeConverterFactory? converterFactory = null,
        ITypeConvertorGenerator? convertorGenerator = null, ISymmetricCipher? cipher = null)
    {
        var serializer = new EventSerializer(writeMapper, symmetricCipher: cipher);
        var meta = serializer.Serialize(out var hasMetadata, value!).ToAsyncSafe();
        var data = meta;
        if (hasMetadata)
        {
            serializer.ProcessMetadataLog(meta);
            data = serializer.Serialize(out hasMetadata, value!).ToAsyncSafe();
            Assert.False(hasMetadata);
        }

        // Repeated serialization must produce identical bytes (encryption uses random nonce)
        if (cipher == null)
            Assert.Equal(data.ToByteArray(), serializer.Serialize(out _, value!).ToByteArray());

        var deserializer = new EventDeserializer(readMapper, converterFactory, convertorGenerator, cipher);
        if (!ReferenceEquals(meta.Buffer, data.Buffer))
        {
            Assert.False(deserializer.Deserialize(out _, data));
            deserializer.ProcessMetadataLog(meta);
        }

        Assert.True(deserializer.Deserialize(out var result, data));
        return result;
    }

    /// Serializes using EventStoreLayer (v1) appending store and reads it back using fresh manager.
    public static object? RoundTrip1(object? value, ITypeNameMapper? writeMapper = null,
        ITypeNameMapper? readMapper = null, TypeSerializersOptions? writeOptions = null,
        TypeSerializersOptions? readOptions = null)
    {
        var events = RoundTrip1Many([value], writeMapper, readMapper, writeOptions, readOptions);
        return events[0];
    }

    public static object?[] RoundTrip1Many(object?[] values, ITypeNameMapper? writeMapper = null,
        ITypeNameMapper? readMapper = null, TypeSerializersOptions? writeOptions = null,
        TypeSerializersOptions? readOptions = null)
    {
        var manager = new EventStoreManager(writeOptions);
        if (writeMapper != null) manager.SetNewTypeNameMapper(writeMapper);
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        appender.Store(null, values!);

        manager = new EventStoreManager(readOptions);
        if (readMapper != null) manager.SetNewTypeNameMapper(readMapper);
        var reader = manager.OpenReadOnlyStore(file);
        var observer = new CollectingEventObserver();
        reader.ReadFromStartToEnd(observer);
        Assert.Single(observer.Events);
        return observer.Events[0];
    }

    /// Serializes using TypeSerializers mapping (v1 layer without event store) and loads into fresh mapping.
    public static object? RoundTripMapping(object? value, ITypeNameMapper? writeMapper = null,
        ITypeNameMapper? readMapper = null, TypeSerializersOptions? writeOptions = null,
        TypeSerializersOptions? readOptions = null)
    {
        var writeTs = new TypeSerializers(writeMapper, writeOptions);
        var writeMapping = writeTs.CreateMapping();
        var writer = new MemWriter();
        var storedDescriptorCtx = writeMapping.StoreNewDescriptors(value);
        storedDescriptorCtx.FinishNewDescriptors(ref writer);
        storedDescriptorCtx.StoreObject(ref writer, value);
        storedDescriptorCtx.CommitNewDescriptors();
        var bytes = writer.GetSpan().ToArray();

        var readTs = new TypeSerializers(readMapper, readOptions);
        var readMapping = readTs.CreateMapping();
        unsafe
        {
            fixed (byte* _ = bytes)
            {
                var reader = MemReader.CreateFromPinnedSpan(bytes);
                readMapping.LoadTypeDescriptors(ref reader);
                var result = readMapping.LoadObject(ref reader);
                Assert.True(reader.Eof);
                return result;
            }
        }
    }

    /// Runs action with all NoEmit flags set to given value. Must be used only from tests in
    /// collection "IFieldHandler.UseNoEmitForRelations" which disables parallelization.
    public static void RunWithEmitMode(bool useNoEmit, Action test)
    {
        var saved = (IFieldHandler.UseNoEmit, IFieldHandler.UseNoEmitForKeyValue,
            IFieldHandler.UseNoEmitForRelations, IFieldHandler.UseNoEmitForDescriptors);
        try
        {
            IFieldHandler.UseNoEmit = useNoEmit;
            IFieldHandler.UseNoEmitForKeyValue = useNoEmit;
            IFieldHandler.UseNoEmitForRelations = useNoEmit;
            IFieldHandler.UseNoEmitForDescriptors = useNoEmit;
            ObjectDB.ResetAllMetadataCaches();
            test();
        }
        finally
        {
            (IFieldHandler.UseNoEmit, IFieldHandler.UseNoEmitForKeyValue,
                IFieldHandler.UseNoEmitForRelations, IFieldHandler.UseNoEmitForDescriptors) = saved;
            ObjectDB.ResetAllMetadataCaches();
        }
    }
}

class CollectingEventObserver : IEventStoreObserver
{
    public readonly List<object?[]> Events = new();

    public bool ObservedMetadata(object metadata, uint eventCount) => true;

    public bool ShouldStopReadingNextEvents() => false;

    public void ObservedEvents(object[] events) => Events.Add(events);
}
