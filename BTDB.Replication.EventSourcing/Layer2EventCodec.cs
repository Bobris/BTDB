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

// Layer2 serialization with type IDs from one metadata file in a versioned blob, bound to the data topic by name.
// Every writer only appends to the file, under the version it read, so type IDs never change. Metadata is durable
// before any data that uses it: a publication writes its new types first, and a consumer missing a type refreshes
// once, after which a still missing type means damaged data. Proposed types are used only once their write applied.
internal sealed class Layer2EventCodec(IVersionedBlob blob, string bindingName, ITypeNameMapper? mapper)
{
    readonly Lock _lock = new();
    readonly SemaphoreSlim _refreshing = new(1);
    CatalogEventSerializer _serializer = null!;
    CatalogEventDeserializer _deserializer = null!;
    string? _version; // of the installed metadata; null while the blob does not exist

    internal async Task LoadAsync(CancellationToken cancellation)
    {
        var (metadata, version) = await ReadAsync(cancellation).ConfigureAwait(false);
        lock (_lock)
        {
            _serializer = new(metadata, mapper, forbidSerializationOfLazyDBObjects: true);
            _deserializer = new(metadata, mapper);
            _version = version;
        }
    }

    async Task<(Layer2Metadata Metadata, string? Version)> ReadAsync(CancellationToken cancellation)
    {
        var content = await blob.ReadAsync(cancellation).ConfigureAwait(false);
        if (content == null) return (Layer2Metadata.Create(bindingName), null);
        var metadata = Layer2Metadata.Load(content.Content);
        if (metadata.BindingName != bindingName)
            throw new InvalidDataException(
                $"The Layer2 metadata belongs to data topic '{metadata.BindingName}', not '{bindingName}'.");
        return (metadata, content.Version);
    }

    /// <summary>Serializes an event, first writing the metadata of its new types. The caller allows one publication
    /// at a time on this node; other nodes may write concurrently.</summary>
    internal async Task<byte[]> SerializeAsync(StoredEvent ev, CancellationToken cancellation)
    {
        while (true)
        {
            Layer2Metadata? proposal;
            string? version;
            lock (_lock)
            {
                var data = _serializer.Serialize(out proposal, ev);
                if (proposal == null) return data.AsSyncReadOnlySpan().ToArray();
                version = _version;
            }
            var written = await blob.WriteAsync(version, proposal.Bytes, cancellation).ConfigureAwait(false);
            if (written.Outcome == VersionedBlobWriteOutcome.Applied) Install(proposal, written.Version!);
            // Another node wrote first, or the outcome is unknown: read what is stored and serialize again. Types are
            // found by shape, so the ones another node or this node's own landed write stored are not written again.
            else await RefreshAsync(version, cancellation).ConfigureAwait(false);
        }
    }

    internal async Task<StoredEvent> DeserializeAsync(ReadOnlyMemory<byte> bytes, CancellationToken cancellation)
    {
        lock (_lock)
            if (TryDeserialize(bytes, out var available)) return available!;
        // Metadata is written before the data that uses it. Re-check after acquiring _refreshing in case another
        // refresh already installed the needed type; otherwise read the blob once (a concurrent local write cannot
        // have satisfied a type written later by another node).
        await _refreshing.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            lock (_lock)
                if (TryDeserialize(bytes, out var available)) return available!;
            var (metadata, version) = await ReadAsync(cancellation).ConfigureAwait(false);
            Install(metadata, version);
            lock (_lock)
                return TryDeserialize(bytes, out var stored) ? stored!
                    : throw new InvalidDataException("Event metadata is missing from the Layer2 metadata file.");
        }
        finally { _refreshing.Release(); }
    }

    bool TryDeserialize(ReadOnlyMemory<byte> bytes, out StoredEvent? stored)
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

    // Reads the blob unless a refresh since knownVersion installed newer metadata already; one read at a time.
    async Task RefreshAsync(string? knownVersion, CancellationToken cancellation)
    {
        await _refreshing.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            lock (_lock)
                if (_version != knownVersion) return;
            var (metadata, version) = await ReadAsync(cancellation).ConfigureAwait(false);
            Install(metadata, version);
        }
        finally { _refreshing.Release(); }
    }

    // Installs metadata unless the installed one is strictly ahead (a refresh can overtake this node's applied write).
    void Install(Layer2Metadata metadata, string? version)
    {
        lock (_lock)
        {
            if (version == _version) return;
            var installed = _serializer.Metadata;
            if ((installed.TypeCount > metadata.TypeCount || installed.StringCount > metadata.StringCount) &&
                installed.Extends(metadata)) return;
            if (version == null || !metadata.Extends(installed))
                throw new InvalidDataException("The Layer2 metadata file changed other than by appending.");
            _serializer.Update(metadata);
            _deserializer.Update(metadata);
            _version = version;
        }
    }
}
