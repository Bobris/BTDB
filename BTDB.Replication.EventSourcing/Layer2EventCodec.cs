using BTDB.Buffer;
using BTDB.EventStore2Layer;
using BTDB.EventStoreLayer;

namespace BTDB.Replication.EventSourcing;

// Consumption can precede the publication receipt. Correlation lets the originating instance return the locally
// committed result in either order; the event's ordered ID is still derived only from its data-topic offset.
[Generate]
internal sealed class StoredEvent
{
    public Guid PublicationId { get; set; }
    public Event Event { get; set; } = null!;
}

// The caller serializes access. Both Layer2 instances receive the complete, ordered metadata topic;
// proposed metadata is never applied optimistically before its actual topic position is known.
internal sealed class Layer2EventCodec(ITypeNameMapper? mapper)
{
    readonly EventSerializer _serializer = new(mapper, useInputDescriptors: false, forbidSerializationOfLazyDBObjects: true);
    readonly EventDeserializer _deserializer = new(mapper);

    internal byte[] Serialize(StoredEvent ev, out bool metadata) =>
        _serializer.Serialize(out metadata, ev).AsSyncReadOnlySpan().ToArray();

    internal void ApplyMetadata(ReadOnlyMemory<byte> bytes)
    {
        var buffer = ByteBuffer.NewAsync(bytes);
        _serializer.ProcessMetadataLog(buffer);
        _deserializer.ProcessMetadataLog(buffer);
    }

    internal bool TryDeserialize(ReadOnlyMemory<byte> bytes, out StoredEvent? stored)
    {
        if (!_deserializer.Deserialize(out var ev, ByteBuffer.NewAsync(bytes)))
        {
            stored = null;
            return false;
        }
        stored = ev as StoredEvent;
        if (stored is not { Event: not null } || stored.PublicationId == Guid.Empty)
            throw new InvalidDataException("Invalid Layer2 event record.");
        return true;
    }
}
