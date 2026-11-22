using System;
using System.Collections.Generic;
using System.IO;
using BTDB.Buffer;
using BTDB.EventStoreLayer;

namespace BTDB.EventStore2Layer;

/// <summary>
/// Builds the descriptors of stored types on demand, for one owner (descriptors are bound to their
/// <see cref="ITypeDescriptorCallbacks"/>). A type is built with every not-yet-built type it reaches; types never asked
/// for are never built. Inline structural types are shared through a map keyed by their encoding: references inside it
/// are absolute type IDs, so equal bytes mean an equal shape. A standalone structural type shares its descriptor with
/// equal inline ones. Not thread-safe; the caller serializes access.
/// </summary>
sealed class Layer2DescriptorDecoder(ITypeDescriptorCallbacks callbacks)
{
    readonly Dictionary<byte[], ITypeDescriptor> _structural = new(ByteContent.Instance);
    ITypeDescriptor?[] _types = [];
    string?[] _strings = [];

    // State of the build in progress.
    Layer2Metadata _metadata = null!;
    readonly Stack<int> _work = new();
    readonly HashSet<int> _queued = new();
    readonly List<int> _built = new();
    readonly List<(byte[] Key, ITypeDescriptor Descriptor)> _created = new();

    /// <summary>Number of distinct structural descriptors built so far.</summary>
    public int StructuralCount => _structural.Count;

    public ITypeDescriptor Get(Layer2Metadata metadata, int typeId)
    {
        if (Layer2Encoding.BuiltIn(typeId) is { } builtIn) return builtIn;
        if (!metadata.ContainsType(typeId)) throw new InvalidDataException("Layer2 metadata has no type " + typeId + ".");
        if (_types.Length < metadata.TypeCount) Array.Resize(ref _types, metadata.TypeCount);
        if (_strings.Length < metadata.StringCount) Array.Resize(ref _strings, metadata.StringCount);
        if (_types[typeId - Layer2Metadata.FirstTypeId] is { } known) return known;
        _metadata = metadata;
        try
        {
            Build(typeId);
        }
        catch
        {
            // A damaged record leaves half its types built; they must not be reused.
            foreach (var id in _built) _types[id - Layer2Metadata.FirstTypeId] = null;
            foreach (var (key, _) in _created) _structural.Remove(key);
            throw;
        }
        finally
        {
            _work.Clear();
            _queued.Clear();
            _built.Clear();
            _created.Clear();
            _metadata = null!;
        }
        return _types[typeId - Layer2Metadata.FirstTypeId]!;
    }

    void Build(int typeId)
    {
        _work.Push(typeId);
        _queued.Add(typeId);
        while (_work.Count > 0)
        {
            var id = _work.Pop();
            _types[id - Layer2Metadata.FirstTypeId] = Decode(id);
            _built.Add(id);
        }
        EnsureStructuralAcyclic();
        // Types referred to before they were built hold placeholders; replace them, inside inline types too.
        var mapped = new HashSet<ITypeDescriptor>(ReferenceEqualityComparer.Instance);
        var created = new HashSet<ITypeDescriptor>(ReferenceEqualityComparer.Instance);
        foreach (var (_, descriptor) in _created) created.Add(descriptor);
        Func<ITypeDescriptor, ITypeDescriptor> map = null!;
        map = descriptor =>
        {
            if (descriptor is PlaceHolderDescriptor placeHolder)
                return _types[placeHolder.TypeId - Layer2Metadata.FirstTypeId]!;
            if (created.Contains(descriptor) && mapped.Add(descriptor)) descriptor.MapNestedTypes(map);
            return descriptor;
        };
        foreach (var id in _built)
        {
            var descriptor = _types[id - Layer2Metadata.FirstTypeId]!;
            if (mapped.Add(descriptor)) descriptor.MapNestedTypes(map);
        }
        // Names of structural types derive from their items; inner ones were created first.
        foreach (var (_, descriptor) in _created) descriptor.MapNestedTypes(d => d);
    }

    // Structural types cannot contain themselves without a class in between; a damaged file can encode a cycle
    // of standalone structural records, which would recurse infinitely while computing names.
    void EnsureStructuralAcyclic()
    {
        Dictionary<ITypeDescriptor, byte>? state = null;
        Stack<(ITypeDescriptor Descriptor, int Child)>? stack = null;
        foreach (var id in _built)
        {
            var root = _types[id - Layer2Metadata.FirstTypeId]!;
            if (!Layer2Encoding.IsStructural(root)) continue;
            state ??= new(ReferenceEqualityComparer.Instance);
            if (state.ContainsKey(root)) continue;
            stack ??= new();
            state[root] = 1;
            stack.Push((root, 0));
            while (stack.Count > 0)
            {
                var (current, childIndex) = stack.Pop();
                if (childIndex == Layer2Encoding.ChildCount(current))
                {
                    state[current] = 2;
                    continue;
                }
                stack.Push((current, childIndex + 1));
                var child = Layer2Encoding.Child(current, childIndex);
                if (child is PlaceHolderDescriptor placeHolder)
                    child = _types[placeHolder.TypeId - Layer2Metadata.FirstTypeId]!;
                if (!Layer2Encoding.IsStructural(child)) continue;
                if (state.TryGetValue(child, out var childState))
                {
                    if (childState == 1)
                        throw new InvalidDataException("Layer2 metadata has a cyclic structural type.");
                    continue;
                }
                state[child] = 1;
                stack.Push((child, 0));
            }
        }
    }

    ITypeDescriptor Decode(int typeId)
    {
        var record = _metadata.Record(typeId);
        var reader = new Layer2Reader(record);
        var category = (TypeCategory)reader.ReadByte();
        ITypeDescriptor result;
        switch (category)
        {
            case TypeCategory.Class:
            {
                var name = String(ref reader);
                var count = reader.ReadCount();
                var fields = new List<KeyValuePair<string, ITypeDescriptor>>(count);
                for (var i = 0; i < count; i++)
                {
                    var fieldName = String(ref reader);
                    fields.Add(new(fieldName, Reference(ref reader, 0)));
                }
                result = new ObjectTypeDescriptor(callbacks, name, fields);
                break;
            }
            case TypeCategory.Enum:
            {
                var name = String(ref reader);
                var header = reader.ReadVUInt64();
                if (header >> 2 > (ulong)reader.Remaining)
                    throw new InvalidDataException("Layer2 metadata record has a wrong count.");
                var signed = (header & 1) != 0;
                var pairs = new List<KeyValuePair<string, ulong>>((int)(header >> 2));
                for (var i = 0UL; i < header >> 2; i++)
                {
                    var memberName = String(ref reader);
                    pairs.Add(new(memberName, signed ? (ulong)reader.ReadVInt64() : reader.ReadVUInt64()));
                }
                result = new EnumTypeDescriptor(callbacks, null, name, signed, (header & 2) != 0, pairs);
                break;
            }
            default:
            {
                // A structural type stored on its own: the same payload as inline, so the same shared descriptor.
                var tag = Layer2Encoding.Tag(category);
                var children = Payload(category, ref reader, 0);
                if (!reader.AtEnd) throw new InvalidDataException("Layer2 metadata record has trailing bytes.");
                var key = new byte[record.Length];
                record.CopyTo(key);
                key[0] = (byte)tag;
                return Structural(key, category, children);
            }
        }
        if (!reader.AtEnd) throw new InvalidDataException("Layer2 metadata record has trailing bytes.");
        return result;
    }

    string String(ref Layer2Reader reader)
    {
        var id = reader.ReadId();
        if (id >= _metadata.StringCount) throw new InvalidDataException("Layer2 metadata record names a missing string.");
        return _strings[id] ??= _metadata.String(id);
    }

    ITypeDescriptor Reference(ref Layer2Reader reader, int depth)
    {
        var start = reader.Position;
        var reference = reader.ReadId();
        if (reference < Layer2Encoding.InlineTags)
        {
            var category = Layer2Encoding.CategoryOfTag(reference);
            var children = Payload(category, ref reader, depth);
            return Structural(reader.Consumed(start).ToArray(), category, children);
        }
        var id = reference - Layer2Encoding.InlineTags;
        if (Layer2Encoding.BuiltIn(id) is { } builtIn) return builtIn;
        if (!_metadata.ContainsType(id)) throw new InvalidDataException("Layer2 metadata record refers to a missing type.");
        if (_types[id - Layer2Metadata.FirstTypeId] is { } known) return known;
        if (_queued.Add(id)) _work.Push(id);
        return new PlaceHolderDescriptor(id);
    }

    ITypeDescriptor[] Payload(TypeCategory category, ref Layer2Reader reader, int depth)
    {
        if (depth > Layer2Encoding.MaxInlineDepth)
            throw new InvalidDataException("Layer2 metadata record nests inline types too deep.");
        var count = category switch
        {
            TypeCategory.List or TypeCategory.Nullable => 1,
            TypeCategory.Dictionary => 2,
            _ => reader.ReadCount()
        };
        if (count == 0) throw new InvalidDataException("Layer2 metadata record has an empty tuple.");
        var children = new ITypeDescriptor[count];
        for (var i = 0; i < count; i++) children[i] = Reference(ref reader, depth + 1);
        return children;
    }

    ITypeDescriptor Structural(byte[] key, TypeCategory category, ITypeDescriptor[] children)
    {
        if (_structural.TryGetValue(key, out var shared)) return shared;
        ITypeDescriptor created = category switch
        {
            TypeCategory.List => new ListTypeDescriptor(callbacks, children[0]),
            TypeCategory.Dictionary => new DictionaryTypeDescriptor(callbacks, children[0], children[1]),
            TypeCategory.Nullable => new NullableTypeDescriptor(callbacks, children[0]),
            _ => new TupleTypeDescriptor(callbacks, children)
        };
        _structural.Add(key, created);
        _created.Add((key, created));
        return created;
    }

    sealed class ByteContent : IEqualityComparer<byte[]>
    {
        public static readonly ByteContent Instance = new();

        public bool Equals(byte[]? x, byte[]? y) => x.AsSpan().SequenceEqual(y);

        public int GetHashCode(byte[] value) => (int)XxHash64.Hash(value);
    }
}
