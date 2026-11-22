using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BTDB.Buffer;
using BTDB.EventStoreLayer;

namespace BTDB.EventStore2Layer;

/// <summary>
/// Finds the stored type IDs of descriptors built from CLR types, appending the types a snapshot lacks.
/// A stored type is found by its shape key and always verified by comparing canonical encodings, so a damaged or
/// colliding key can only cause a duplicate append, never a wrong ID.
/// The canonical encoding of a type is its record with names as length-prefixed UTF-8 bytes instead of string IDs, so
/// it needs no string lookup. Its references are varints: 0–3 inline structural types as in records, 4 a reference
/// to the member of the same cyclic group with the following local index, 5 such a reference masked for ordering
/// the group, any other value a type ID + 6.
/// Types are resolved bottom-up one strongly connected group at a time. A non-cyclic type's key hashes its encoding.
/// A cyclic group is ordered by its members' masked encodings, its members are encoded with local references, and
/// member i's key hashes the group hash with seed i. New groups take consecutive IDs in that order, so a stored group
/// is verified from its first member's candidates.
/// </summary>
sealed class Layer2TypeResolver
{
    const int LocalRef = Layer2Encoding.InlineTags;
    const int MaskedRef = Layer2Encoding.InlineTags + 1;
    const int CanonicalIdBase = Layer2Encoding.InlineTags + 2;

    readonly Layer2MetadataBuilder _builder;
    readonly Dictionary<ITypeDescriptor, int> _ids = new(ReferenceEqualityComparer.Instance);
    readonly ArrayBufferWriter<byte> _scratch = new();

    public Layer2TypeResolver(Layer2Metadata metadata) => _builder = new(metadata);

    /// <summary>True when a resolved type was not stored yet; <see cref="Build"/> then writes the next file.</summary>
    public bool HasNewTypes => _builder.HasNewTypes;

    public Layer2Metadata Build() => _builder.Build();

    /// <summary>The type ID of a class, an enum, a built-in type or a structural type stored on its own.</summary>
    public int Resolve(ITypeDescriptor descriptor)
    {
        if (Layer2Encoding.TryGetBuiltInId(descriptor, out var id)) return id;
        if (_ids.TryGetValue(descriptor, out id)) return id;
        new StronglyConnected(this).Visit(descriptor);
        return _ids[descriptor];
    }

    // Named types an entry refers to; inline structural types are looked through.
    static List<ITypeDescriptor> References(ITypeDescriptor entry)
    {
        var result = new List<ITypeDescriptor>();

        void Collect(ITypeDescriptor descriptor)
        {
            if (Layer2Encoding.TryGetBuiltInId(descriptor, out _)) return;
            if (!Layer2Encoding.IsStructural(descriptor))
            {
                result.Add(descriptor);
                return;
            }
            for (var i = 0; i < Layer2Encoding.ChildCount(descriptor); i++) Collect(Layer2Encoding.Child(descriptor, i));
        }

        switch (entry)
        {
            case ObjectTypeDescriptor o:
                foreach (var field in o.Fields) Collect(field.Value);
                break;
            case EnumTypeDescriptor:
                break;
            default:
                Layer2Encoding.Category(entry);
                for (var i = 0; i < Layer2Encoding.ChildCount(entry); i++) Collect(Layer2Encoding.Child(entry, i));
                break;
        }
        return result;
    }

    // Tarjan's algorithm; groups complete children first, which is the order they are resolved in.
    sealed class StronglyConnected(Layer2TypeResolver owner)
    {
        readonly Dictionary<ITypeDescriptor, (int Index, int Low)> _state = new(ReferenceEqualityComparer.Instance);
        readonly HashSet<ITypeDescriptor> _onStack = new(ReferenceEqualityComparer.Instance);
        readonly Stack<ITypeDescriptor> _stack = new();
        readonly Dictionary<ITypeDescriptor, List<ITypeDescriptor>> _references = new(ReferenceEqualityComparer.Instance);
        int _next;

        List<ITypeDescriptor> ReferencesOf(ITypeDescriptor entry)
        {
            if (!_references.TryGetValue(entry, out var references))
                _references[entry] = references = References(entry);
            return references;
        }

        public void Visit(ITypeDescriptor entry)
        {
            EventNesting.EnsureStack("store");
            _state[entry] = (_next, _next);
            _next++;
            _stack.Push(entry);
            _onStack.Add(entry);
            foreach (var reference in ReferencesOf(entry))
            {
                if (owner._ids.ContainsKey(reference)) continue;
                if (!_state.TryGetValue(reference, out var state))
                {
                    Visit(reference);
                    _state[entry] = (_state[entry].Index, Math.Min(_state[entry].Low, _state[reference].Low));
                }
                else if (_onStack.Contains(reference))
                    _state[entry] = (_state[entry].Index, Math.Min(_state[entry].Low, state.Index));
            }
            if (_state[entry].Low != _state[entry].Index) return;
            var group = new List<ITypeDescriptor>();
            ITypeDescriptor member;
            do
            {
                member = _stack.Pop();
                _onStack.Remove(member);
                group.Add(member);
            } while (!ReferenceEquals(member, entry));
            group.Reverse(); // discovery order, the tie-break for ordering the group
            if (group.Count == 1 && !ReferencesOf(entry).Contains(entry, ReferenceEqualityComparer.Instance))
                owner.ResolveSingle(entry);
            else
                owner.ResolveCycle(group);
        }
    }

    static uint Key(ReadOnlySpan<byte> canonical) => (uint)XxHash64.Hash(canonical);

    void ResolveSingle(ITypeDescriptor descriptor)
    {
        var canonical = Canonical(descriptor, null, false);
        var key = Key(canonical);
        foreach (var candidate in _builder.TypesWithKey(key))
        {
            if (!StoredCanonicalEquals(candidate, 0, 0, canonical)) continue;
            _ids[descriptor] = candidate;
            return;
        }
        var id = _builder.ReserveType();
        _ids[descriptor] = id;
        _builder.SetType(id, Stored(descriptor), key);
    }

    void ResolveCycle(List<ITypeDescriptor> group)
    {
        var members = new Dictionary<ITypeDescriptor, int>(ReferenceEqualityComparer.Instance);
        foreach (var member in group) members[member] = 0;
        var masked = group.Select(member => Canonical(member, members, true)).ToArray();
        var order = Enumerable.Range(0, group.Count).ToArray();
        // Stable: equal masked encodings keep discovery order. Descriptors built from CLR types have one name per type,
        // so ties do not occur; one would cost a duplicate append, not a wrong ID.
        Array.Sort(order, (a, b) =>
        {
            var compared = masked[a].AsSpan().SequenceCompareTo(masked[b]);
            return compared != 0 ? compared : a.CompareTo(b);
        });
        var ordered = order.Select(i => group[i]).ToArray();
        for (var i = 0; i < ordered.Length; i++) members[ordered[i]] = i;
        var encodings = ordered.Select(member => Canonical(member, members, false)).ToArray();
        var concatenated = new ArrayBufferWriter<byte>();
        foreach (var encoding in encodings) concatenated.WriteName(encoding);
        var groupHash = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(groupHash, XxHash64.Hash(concatenated.WrittenSpan));
        var keys = Enumerable.Range(0, ordered.Length).Select(i => (uint)XxHash64.Hash(groupHash, (ulong)i)).ToArray();
        foreach (var first in _builder.TypesWithKey(keys[0]))
        {
            if (!_builder.ContainsType(first + ordered.Length - 1)) continue;
            var matches = true;
            for (var i = 0; i < ordered.Length && matches; i++)
                matches = StoredCanonicalEquals(first + i, first, ordered.Length, encodings[i]);
            if (!matches) continue;
            for (var i = 0; i < ordered.Length; i++) _ids[ordered[i]] = first + i;
            return;
        }
        var ids = new int[ordered.Length];
        for (var i = 0; i < ordered.Length; i++)
        {
            ids[i] = _builder.ReserveType();
            _ids[ordered[i]] = ids[i];
        }
        for (var i = 0; i < ordered.Length; i++) _builder.SetType(ids[i], Stored(ordered[i]), keys[i]);
    }

    byte[] Canonical(ITypeDescriptor descriptor, Dictionary<ITypeDescriptor, int>? group, bool masked)
    {
        var writer = new ArrayBufferWriter<byte>();
        var category = Layer2Encoding.Category(descriptor);
        writer.WriteByte((byte)category);
        switch (descriptor)
        {
            case ObjectTypeDescriptor o:
                writer.WriteName(o.Name);
                var fields = o.Fields.ToList();
                writer.WriteVUInt((ulong)fields.Count);
                foreach (var (name, type) in fields)
                {
                    writer.WriteName(name);
                    CanonicalRef(writer, type, group, masked);
                }
                break;
            case EnumTypeDescriptor e:
                writer.WriteName(e.Name);
                writer.WriteVUInt(Layer2Encoding.EnumHeader(e));
                foreach (var (name, value) in e.Pairs)
                {
                    writer.WriteName(name);
                    if (e.Signed) writer.WriteVInt((long)value);
                    else writer.WriteVUInt(value);
                }
                break;
            default:
                CanonicalPayload(writer, descriptor, group, masked);
                break;
        }
        return writer.WrittenSpan.ToArray();
    }

    void CanonicalPayload(ArrayBufferWriter<byte> writer, ITypeDescriptor structural,
        Dictionary<ITypeDescriptor, int>? group, bool masked)
    {
        var count = Layer2Encoding.ChildCount(structural);
        if (structural is TupleTypeDescriptor) writer.WriteVUInt((ulong)count);
        for (var i = 0; i < count; i++) CanonicalRef(writer, Layer2Encoding.Child(structural, i), group, masked);
    }

    void CanonicalRef(ArrayBufferWriter<byte> writer, ITypeDescriptor type, Dictionary<ITypeDescriptor, int>? group,
        bool masked)
    {
        if (Layer2Encoding.TryGetBuiltInId(type, out var id))
            writer.WriteVUInt((ulong)(id + CanonicalIdBase));
        else if (Layer2Encoding.IsStructural(type))
        {
            writer.WriteVUInt((ulong)Layer2Encoding.Tag(Layer2Encoding.Category(type)));
            CanonicalPayload(writer, type, group, masked);
        }
        else if (group != null && group.TryGetValue(type, out var local))
        {
            if (masked) writer.WriteVUInt(MaskedRef);
            else
            {
                writer.WriteVUInt(LocalRef);
                writer.WriteVUInt((ulong)local);
            }
        }
        else writer.WriteVUInt((ulong)(_ids[type] + CanonicalIdBase));
    }

    byte[] Stored(ITypeDescriptor descriptor)
    {
        var writer = new ArrayBufferWriter<byte>();
        writer.WriteByte((byte)Layer2Encoding.Category(descriptor));
        switch (descriptor)
        {
            case ObjectTypeDescriptor o:
                writer.WriteVUInt((ulong)_builder.GetOrAddString(o.Name));
                var fields = o.Fields.ToList();
                writer.WriteVUInt((ulong)fields.Count);
                foreach (var (name, type) in fields)
                {
                    writer.WriteVUInt((ulong)_builder.GetOrAddString(name));
                    StoredRef(writer, type);
                }
                break;
            case EnumTypeDescriptor e:
                writer.WriteVUInt((ulong)_builder.GetOrAddString(e.Name));
                writer.WriteVUInt(Layer2Encoding.EnumHeader(e));
                foreach (var (name, value) in e.Pairs)
                {
                    writer.WriteVUInt((ulong)_builder.GetOrAddString(name));
                    if (e.Signed) writer.WriteVInt((long)value);
                    else writer.WriteVUInt(value);
                }
                break;
            default:
                StoredPayload(writer, descriptor);
                break;
        }
        return writer.WrittenSpan.ToArray();
    }

    void StoredPayload(ArrayBufferWriter<byte> writer, ITypeDescriptor structural)
    {
        var count = Layer2Encoding.ChildCount(structural);
        if (structural is TupleTypeDescriptor) writer.WriteVUInt((ulong)count);
        for (var i = 0; i < count; i++) StoredRef(writer, Layer2Encoding.Child(structural, i));
    }

    void StoredRef(ArrayBufferWriter<byte> writer, ITypeDescriptor type)
    {
        if (Layer2Encoding.TryGetBuiltInId(type, out var id))
            writer.WriteVUInt((ulong)(id + Layer2Encoding.InlineTags));
        else if (Layer2Encoding.IsStructural(type))
        {
            writer.WriteVUInt((ulong)Layer2Encoding.Tag(Layer2Encoding.Category(type)));
            StoredPayload(writer, type);
        }
        else writer.WriteVUInt((ulong)(_ids[type] + Layer2Encoding.InlineTags));
    }

    // Writes the canonical encoding of a stored record, with references into [groupStart, groupStart + groupCount)
    // as local ones, and compares it.
    bool StoredCanonicalEquals(int typeId, int groupStart, int groupCount, ReadOnlySpan<byte> expected)
    {
        _scratch.Clear();
        var reader = new Layer2Reader(_builder.Record(typeId));
        var category = (TypeCategory)reader.ReadByte();
        _scratch.WriteByte((byte)category);
        switch (category)
        {
            case TypeCategory.Class:
                _scratch.WriteName(StoredString(ref reader));
                var fields = reader.ReadCount();
                _scratch.WriteVUInt((ulong)fields);
                for (var i = 0; i < fields; i++)
                {
                    _scratch.WriteName(StoredString(ref reader));
                    StoredRefToCanonical(ref reader, groupStart, groupCount, 0);
                }
                break;
            case TypeCategory.Enum:
                _scratch.WriteName(StoredString(ref reader));
                var header = reader.ReadVUInt64();
                if (header >> 2 > (ulong)reader.Remaining)
                    throw new InvalidDataException("Layer2 metadata record has a wrong count.");
                _scratch.WriteVUInt(header);
                for (var i = 0UL; i < header >> 2; i++)
                {
                    _scratch.WriteName(StoredString(ref reader));
                    if ((header & 1) != 0) _scratch.WriteVInt(reader.ReadVInt64());
                    else _scratch.WriteVUInt(reader.ReadVUInt64());
                }
                break;
            default:
                StoredPayloadToCanonical(ref reader, category, groupStart, groupCount, 0);
                break;
        }
        if (!reader.AtEnd) throw new InvalidDataException("Layer2 metadata record has trailing bytes.");
        return _scratch.WrittenSpan.SequenceEqual(expected);
    }

    ReadOnlySpan<byte> StoredString(ref Layer2Reader reader)
    {
        var id = reader.ReadId();
        if (id >= _builder.StringCount) throw new InvalidDataException("Layer2 metadata record names a missing string.");
        return _builder.StringUtf8(id);
    }

    void StoredPayloadToCanonical(ref Layer2Reader reader, TypeCategory category, int groupStart, int groupCount,
        int depth)
    {
        if (depth > Layer2Encoding.MaxInlineDepth)
            throw new InvalidDataException("Layer2 metadata record nests inline types too deep.");
        int count;
        switch (category)
        {
            case TypeCategory.List or TypeCategory.Nullable:
                count = 1;
                break;
            case TypeCategory.Dictionary:
                count = 2;
                break;
            case TypeCategory.Tuple:
                count = reader.ReadCount();
                _scratch.WriteVUInt((ulong)count);
                break;
            default:
                throw new InvalidDataException("Layer2 metadata has a wrong type category.");
        }
        for (var i = 0; i < count; i++) StoredRefToCanonical(ref reader, groupStart, groupCount, depth + 1);
    }

    void StoredRefToCanonical(ref Layer2Reader reader, int groupStart, int groupCount, int depth)
    {
        var reference = reader.ReadId();
        if (reference < Layer2Encoding.InlineTags)
        {
            _scratch.WriteVUInt((ulong)reference);
            StoredPayloadToCanonical(ref reader, Layer2Encoding.CategoryOfTag(reference), groupStart, groupCount, depth);
            return;
        }
        var id = reference - Layer2Encoding.InlineTags;
        if (Layer2Encoding.BuiltIn(id) == null && !_builder.ContainsType(id))
            throw new InvalidDataException("Layer2 metadata record refers to a missing type.");
        if (id >= groupStart && id - groupStart < groupCount)
        {
            _scratch.WriteVUInt(LocalRef);
            _scratch.WriteVUInt((ulong)(id - groupStart));
        }
        else _scratch.WriteVUInt((ulong)(id + CanonicalIdBase));
    }
}
