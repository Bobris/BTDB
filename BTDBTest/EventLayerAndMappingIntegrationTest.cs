using System;
using System.Collections.Generic;
using System.IO;
using BTDB.EventStoreLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class EventLayerAndMappingIntegrationTest
{
    internal const byte TypeDescriptor = 99;
    internal const byte Nothing = 100;

    [Fact]
    public unsafe void EventLayerCanSerializeDataThatWasDeserializedByMapping()
    {
        var writer = new MemWriter();
        SerializeWithMapping(ref writer, new Event() { Items = new List<EventItem> { new EventItem() } });
        var writtenSpan = writer.GetSpanAndReset();
        fixed (void* _ = writtenSpan)
        {
            var reader = MemReader.CreateFromPinnedSpan(writtenSpan);
            var deserialized = DeserializeWithMapping(ref reader);

            var els = new BTDB.EventStore2Layer.EventSerializer(null, null, null, false);
            els.Serialize(out bool metadata, deserialized);
        }
    }

    void SerializeWithMapping(ref MemWriter writer, object @object)
    {
        var typeSerializer = new TypeSerializers(new SillyTypeNameMapper(),
            new TypeSerializersOptions { IgnoreIIndirect = false, SymmetricCipher = null });
        var mapping = typeSerializer.CreateMapping();
        var start = writer.GetCurrentPosition();

        writer.WriteUInt8(Nothing);
        var serializerContext = mapping.StoreNewDescriptors(@object);
        serializerContext.FinishNewDescriptors(ref writer);
        serializerContext.StoreObject(ref writer, @object);
        if (serializerContext.SomeTypeStored)
        {
            var end = writer.GetCurrentPosition();
            writer.SetCurrentPosition(start);
            writer.WriteUInt8(TypeDescriptor);
            writer.SetCurrentPosition(end);
        }

        serializerContext.CommitNewDescriptors();
    }

    object DeserializeWithMapping(ref MemReader reader)
    {
        var typeSerializer = new TypeSerializers(new SillyTypeNameMapper(),
            new TypeSerializersOptions { IgnoreIIndirect = false, SymmetricCipher = null });
        var mapping = typeSerializer.CreateMapping();
        byte c0 = reader.ReadUInt8();
        if (c0 == TypeDescriptor)
        {
            mapping.LoadTypeDescriptors(ref reader);
        }
        else if (c0 != Nothing)
        {
            throw new InvalidDataException("Data received from other side must Start with byte 99 or 100");
        }

        var obj = mapping.LoadObject(ref reader);
        return obj!;
    }

    class SillyTypeNameMapper : ITypeNameMapper
    {
        FullNameTypeMapper _fullNameTypeMapper = new();

        public string ToName(Type type)
        {
            if (type == typeof(Event))
                return "WrongName";
            if (type == typeof(EventItem))
                return "AlsoWrongName";

            return _fullNameTypeMapper.ToName(type);
        }

        public Type? ToType(string name)
        {
            if (name == "WrongName")
                return typeof(Event);
            if (name == "AlsoWrongName")
                return typeof(EventItem);

            return _fullNameTypeMapper.ToType(name);
        }
    }

    class Event
    {
        public IList<EventItem> Items { get; set; }
    }

    public class EventItem
    {
    }
}
