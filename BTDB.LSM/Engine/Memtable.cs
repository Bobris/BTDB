using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.LSM.Format;
using BTDB.LSM.Memory;

namespace BTDB.LSM.Engine;

sealed class MemNode
{
    public required byte[] Key;
    public byte[] Value = [];
    public bool IsSet;
    public bool ExistsBelow;
    public MemNode? Left;
    public MemNode? Right;
    public uint Priority;
    public int Count;
    public long DeltaSum;
    public long Owner;

    public int Delta => (IsSet ? 1 : 0) - (ExistsBelow ? 1 : 0);

    public MemNode Clone(long owner) => new()
    {
        Key = Key, Value = Value, IsSet = IsSet, ExistsBelow = ExistsBelow, Left = Left, Right = Right,
        Priority = Priority, Count = Count, DeltaSum = DeltaSum, Owner = owner
    };
}

/// Mutable counted component of the newest data (ImplementationPlan.md, section 7.1). A writing transaction modifies a
/// private copy; committed snapshots never change.
interface IMemtable : ILsmComponent
{
    /// Bytes counted against MemtableSize.
    long ApproximateBytes { get; }

    bool IsEmpty { get; }

    /// Native memory released together with the memtable, or null for a managed memtable.
    NativeArena? Arena { get; }

    /// Returns a copy that the given owner may modify; this instance stays unchanged.
    IMemtable CloneForWriting(long owner);

    bool TryGetNode(ReadOnlySpan<byte> key, out bool isSet, out bool existsBelow);

    /// Changes an existing point; returns false if the key has no point in this memtable.
    bool UpdateExisting(ReadOnlySpan<byte> key, bool isSet, ReadOnlySpan<byte> value);

    void Insert(byte[] key, bool isSet, bool existsBelow, ReadOnlySpan<byte> value);
    void Remove(ReadOnlySpan<byte> key);

    /// Removes all points in [start, end).
    void RemoveRange(ReadOnlySpan<byte> start, ReadOnlySpan<byte> end);

    void SetTombstones(TombstoneSet tombstones);
}

/// Managed counted copy-on-write memtable: a persistent treap whose nodes aggregate entry count and delta sum, plus
/// tombstone fragments. Nodes created by the writing owner are mutated in place, all others are copied on write. Kept
/// as a reference implementation; NativeMemtable is the default (see BTDB.LSM/Measurements.md).
sealed class Memtable : IMemtable
{
    public const int VirtualBlockSize = 64;
    public const int FinePivotStride = 8;

    MemNode? _root;
    TombstoneSet _tombstones;
    long _owner;

    Memtable(MemNode? root, TombstoneSet tombstones, long approximateBytes, long owner)
    {
        _root = root;
        _tombstones = tombstones;
        ApproximateBytes = approximateBytes;
        _owner = owner;
    }

    public static Memtable CreateEmpty() => new(null, TombstoneSet.Empty, 0, 0);

    public IMemtable CloneForWriting(long owner) => new Memtable(_root, _tombstones, ApproximateBytes, owner);

    public NativeArena? Arena => null;

    public long ApproximateBytes { get; private set; }
    public bool IsEmpty => _root == null && _tombstones.Count == 0;
    public long EntryCount => _root?.Count ?? 0;
    public long DeltaSum => _root?.DeltaSum ?? 0;
    public TombstoneSet Tombstones => _tombstones;

    static int Count(MemNode? node) => node?.Count ?? 0;
    static long Sum(MemNode? node) => node?.DeltaSum ?? 0;

    static void Update(MemNode node)
    {
        node.Count = 1 + Count(node.Left) + Count(node.Right);
        node.DeltaSum = node.Delta + Sum(node.Left) + Sum(node.Right);
    }

    MemNode Own(MemNode node) => node.Owner == _owner ? node : node.Clone(_owner);

    void CheckWritable()
    {
        if (_owner == 0) throw new InvalidOperationException("Memtable snapshot is read-only.");
    }

    // Splits into keys lower than key (or lower or equal when keyGoesLeft) and the rest.
    (MemNode? Left, MemNode? Right) Split(MemNode? node, ReadOnlySpan<byte> key, bool keyGoesLeft)
    {
        if (node == null) return (null, null);
        var cmp = LsmKeys.Compare(node.Key, key);
        if (cmp < 0 || keyGoesLeft && cmp == 0)
        {
            var (left, right) = Split(node.Right, key, keyGoesLeft);
            node = Own(node);
            node.Right = left;
            Update(node);
            return (node, right);
        }
        else
        {
            var (left, right) = Split(node.Left, key, keyGoesLeft);
            node = Own(node);
            node.Left = right;
            Update(node);
            return (left, node);
        }
    }

    MemNode? Merge(MemNode? left, MemNode? right)
    {
        if (left == null) return right;
        if (right == null) return left;
        if (left.Priority > right.Priority)
        {
            left = Own(left);
            left.Right = Merge(left.Right, right);
            Update(left);
            return left;
        }

        right = Own(right);
        right.Left = Merge(left, right.Left);
        Update(right);
        return right;
    }

    MemNode? Find(ReadOnlySpan<byte> key)
    {
        var node = _root;
        while (node != null)
        {
            var cmp = LsmKeys.Compare(node.Key, key);
            if (cmp == 0) return node;
            node = cmp < 0 ? node.Right : node.Left;
        }

        return null;
    }

    public bool TryGetNode(ReadOnlySpan<byte> key, out bool isSet, out bool existsBelow)
    {
        var node = Find(key);
        isSet = node?.IsSet ?? false;
        existsBelow = node?.ExistsBelow ?? false;
        return node != null;
    }

    /// Changes an existing point; returns false if the key has no point in this memtable.
    public bool UpdateExisting(ReadOnlySpan<byte> key, bool isSet, ReadOnlySpan<byte> value)
    {
        CheckWritable();
        if (Find(key) is not { } existing) return false;
        ApproximateBytes += value.Length - existing.Value.Length;
        _root = UpdatePath(_root!, key, isSet, value.ToArray());
        return true;
    }

    MemNode UpdatePath(MemNode node, ReadOnlySpan<byte> key, bool isSet, byte[] value)
    {
        node = Own(node);
        var cmp = LsmKeys.Compare(node.Key, key);
        if (cmp == 0)
        {
            node.IsSet = isSet;
            node.Value = value;
        }
        else if (cmp < 0) node.Right = UpdatePath(node.Right!, key, isSet, value);
        else node.Left = UpdatePath(node.Left!, key, isSet, value);

        Update(node);
        return node;
    }

    public void Insert(byte[] key, bool isSet, bool existsBelow, ReadOnlySpan<byte> value)
    {
        CheckWritable();
        var node = new MemNode
        {
            Key = key, Value = value.ToArray(), IsSet = isSet, ExistsBelow = existsBelow,
            Priority = (uint)XxHash64.Hash(key, 0x9e3779b97f4a7c15), Owner = _owner
        };
        Update(node);
        var (left, right) = Split(_root, key, false);
        _root = Merge(Merge(left, node), right);
        ApproximateBytes += key.Length + value.Length + 96;
    }

    public void Remove(ReadOnlySpan<byte> key)
    {
        CheckWritable();
        var (left, rest) = Split(_root, key, false);
        var (middle, right) = Split(rest, key, true);
        if (middle != null) ApproximateBytes -= middle.Key.Length + middle.Value.Length + 96;
        _root = Merge(left, right);
    }

    /// Removes all points in [start, end).
    public void RemoveRange(ReadOnlySpan<byte> start, ReadOnlySpan<byte> end)
    {
        CheckWritable();
        var (left, rest) = Split(_root, start, false);
        var (middle, right) = Split(rest, end, false);
        if (middle != null)
            foreach (var node in InOrder(middle))
                ApproximateBytes -= node.Key.Length + node.Value.Length + 96;
        _root = Merge(left, right);
    }

    public void SetTombstones(TombstoneSet tombstones)
    {
        CheckWritable();
        _tombstones = tombstones;
        ApproximateBytes += 64;
    }

    public long PointDeltaBefore(ReadOnlySpan<byte> key)
    {
        long sum = 0;
        var node = _root;
        while (node != null)
        {
            if (LsmKeys.Compare(node.Key, key) < 0)
            {
                sum += Sum(node.Left) + node.Delta;
                node = node.Right;
            }
            else node = node.Left;
        }

        return sum;
    }

    public (long Min, long Max) PointDeltaBounds(ReadOnlySpan<byte> key)
    {
        var exact = PointDeltaBefore(key);
        return (exact, exact);
    }

    long CountBefore(ReadOnlySpan<byte> key, bool inclusive)
    {
        long count = 0;
        var node = _root;
        while (node != null)
        {
            var cmp = LsmKeys.Compare(node.Key, key);
            if (cmp < 0 || inclusive && cmp == 0)
            {
                count += Count(node.Left) + 1;
                node = node.Right;
            }
            else node = node.Left;
        }

        return count;
    }

    MemNode NodeAt(long index)
    {
        var node = _root!;
        while (true)
        {
            var leftCount = Count(node.Left);
            if (index < leftCount) node = node.Left!;
            else if (index == leftCount) return node;
            else
            {
                index -= leftCount + 1;
                node = node.Right!;
            }
        }
    }

    public int BlockCount => (int)((EntryCount + VirtualBlockSize - 1) / VirtualBlockSize);

    public byte[] BlockFirstKey(int block) => NodeAt((long)block * VirtualBlockSize).Key;

    public int CountBlocksBefore(ReadOnlySpan<byte> key, bool inclusive) =>
        (int)((CountBefore(key, inclusive) + VirtualBlockSize - 1) / VirtualBlockSize);

    /// Every FinePivotStride-th key of the virtual blocks; ranks in the memtable are exact, so this only bounds the
    /// final linear step of select.
    public void AddFinePivots(int fromBlock, int toBlock, List<byte[]> pivots)
    {
        var end = Math.Min((long)toBlock * VirtualBlockSize, EntryCount);
        for (var index = (long)Math.Max(0, fromBlock) * VirtualBlockSize; index < end; index += FinePivotStride)
            pivots.Add(NodeAt(index).Key);
    }

    static LsmEntry ToEntry(MemNode node) => new(node.Key, node.IsSet, node.ExistsBelow, node.Value);

    public bool TryGet(ReadOnlySpan<byte> key, out LsmEntry entry)
    {
        var node = Find(key);
        entry = node == null ? default : ToEntry(node);
        return node != null;
    }

    public bool SeekGE(ReadOnlySpan<byte> key, bool inclusive, out LsmEntry entry)
    {
        MemNode? best = null;
        var node = _root;
        while (node != null)
        {
            var cmp = LsmKeys.Compare(node.Key, key);
            if (cmp > 0 || inclusive && cmp == 0)
            {
                best = node;
                node = node.Left;
            }
            else node = node.Right;
        }

        entry = best == null ? default : ToEntry(best);
        return best != null;
    }

    public bool SeekLE(ReadOnlySpan<byte> key, bool inclusive, out LsmEntry entry)
    {
        MemNode? best = null;
        var node = _root;
        while (node != null)
        {
            var cmp = LsmKeys.Compare(node.Key, key);
            if (cmp < 0 || inclusive && cmp == 0)
            {
                best = node;
                node = node.Right;
            }
            else node = node.Left;
        }

        entry = best == null ? default : ToEntry(best);
        return best != null;
    }

    public bool SeekLast(out LsmEntry entry)
    {
        var node = _root;
        while (node?.Right != null) node = node.Right;
        entry = node == null ? default : ToEntry(node);
        return node != null;
    }

    static IEnumerable<MemNode> InOrder(MemNode? root)
    {
        var stack = new Stack<MemNode>();
        var node = root;
        while (node != null || stack.Count > 0)
        {
            while (node != null)
            {
                stack.Push(node);
                node = node.Left;
            }

            node = stack.Pop();
            yield return node;
            node = node.Right;
        }
    }

    public IEnumerable<LsmEntry> Enumerate()
    {
        foreach (var node in InOrder(_root)) yield return ToEntry(node);
    }

    public IComponentCursor CreateCursor() => new SeekingComponentCursor(this);
}
