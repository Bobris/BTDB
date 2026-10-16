using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BTDB.LSM.Format;
using BTDB.LSM.Memory;

namespace BTDB.LSM.Engine;

/// Counted copy-on-write B+tree in native memory (ImplementationPlan.md, section 7.1). Nodes, keys and values live in a
/// NativeArena shared by every version of the memtable. Branch entries aggregate the entry count and delta sum of their
/// child for rank and select. A writing owner mutates nodes it created in place and copies the others on first touch,
/// so published versions never change. Nothing is freed individually; the arena is released with the memtable.
sealed unsafe class NativeMemtable : IMemtable
{
    const int Capacity = 32;
    const int VirtualBlockSize = 64;
    const int FinePivotStride = 8;

    [StructLayout(LayoutKind.Sequential)]
    struct Node
    {
        public int IsLeaf;
        public int Count;
        public long Owner;
        public long EntryCount;
        public long DeltaSum;
    }

    [StructLayout(LayoutKind.Sequential)]
    struct LeafEntry
    {
        public byte* Key;
        public byte* Value;
        public int KeyLength;
        public int ValueLength;
        public int Flags;
        public int Padding;

        public readonly int Delta => (Flags & 1) - ((Flags >> 1) & 1);
        public readonly ReadOnlySpan<byte> KeySpan => new(Key, KeyLength);
    }

    [StructLayout(LayoutKind.Sequential)]
    struct BranchEntry
    {
        public Node* Child;
        public byte* FirstKey;
        public int FirstKeyLength;
        public int Padding;
        public long Count;
        public long DeltaSum;

        public readonly ReadOnlySpan<byte> FirstKeySpan => new(FirstKey, FirstKeyLength);
    }

    readonly NativeArena _arena;
    readonly long _owner;
    Node* _root;
    TombstoneSet _tombstones;

    NativeMemtable(NativeArena arena, Node* root, TombstoneSet tombstones, long owner)
    {
        _arena = arena;
        _root = root;
        _tombstones = tombstones;
        _owner = owner;
    }

    public static NativeMemtable CreateEmpty(long memtableSize) =>
        new(new((int)Math.Clamp(memtableSize / 16, 16 * 1024, 1024 * 1024)), null, TombstoneSet.Empty, 0);

    public NativeArena Arena => _arena;
    public long ApproximateBytes => _arena.UsedBytes;
    public bool IsEmpty => EntryCount == 0 && _tombstones.Count == 0;
    public long EntryCount => _root == null ? 0 : _root->EntryCount;
    public long DeltaSum => _root == null ? 0 : _root->DeltaSum;
    public TombstoneSet Tombstones => _tombstones;

    public IMemtable CloneForWriting(long owner) => new NativeMemtable(_arena, _root, _tombstones, owner);

    static LeafEntry* Leaves(Node* node) => (LeafEntry*)(node + 1);
    static BranchEntry* Branches(Node* node) => (BranchEntry*)(node + 1);
    static int NodeSize(bool leaf) => sizeof(Node) + Capacity * (leaf ? sizeof(LeafEntry) : sizeof(BranchEntry));

    void CheckWritable()
    {
        if (_owner == 0) throw new InvalidOperationException("Memtable snapshot is read-only.");
    }

    Node* NewNode(bool leaf)
    {
        var node = (Node*)_arena.Allocate(NodeSize(leaf));
        node->IsLeaf = leaf ? 1 : 0;
        node->Count = 0;
        node->Owner = _owner;
        node->EntryCount = 0;
        node->DeltaSum = 0;
        return node;
    }

    Node* Own(Node* node)
    {
        if (node->Owner == _owner) return node;
        var size = NodeSize(node->IsLeaf != 0);
        var copy = (Node*)_arena.Allocate(size);
        System.Buffer.MemoryCopy(node, copy, size, size);
        copy->Owner = _owner;
        return copy;
    }

    static void Recompute(Node* node)
    {
        long count = 0, delta = 0;
        if (node->IsLeaf != 0)
        {
            var entries = Leaves(node);
            for (var i = 0; i < node->Count; i++) delta += entries[i].Delta;
            count = node->Count;
        }
        else
        {
            var entries = Branches(node);
            for (var i = 0; i < node->Count; i++)
            {
                count += entries[i].Count;
                delta += entries[i].DeltaSum;
            }
        }

        node->EntryCount = count;
        node->DeltaSum = delta;
    }

    static void SetChild(Node* parent, int index, Node* child)
    {
        ref var entry = ref Branches(parent)[index];
        entry.Child = child;
        if (child->IsLeaf != 0)
        {
            entry.FirstKey = Leaves(child)[0].Key;
            entry.FirstKeyLength = Leaves(child)[0].KeyLength;
        }
        else
        {
            entry.FirstKey = Branches(child)[0].FirstKey;
            entry.FirstKeyLength = Branches(child)[0].FirstKeyLength;
        }

        entry.Count = child->EntryCount;
        entry.DeltaSum = child->DeltaSum;
    }

    /// Index of the first leaf entry with key greater than (or equal to, when inclusive) key.
    static int LeafLowerBound(Node* leaf, ReadOnlySpan<byte> key, bool inclusive)
    {
        var entries = Leaves(leaf);
        int lo = 0, hi = leaf->Count;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            var cmp = entries[mid].KeySpan.SequenceCompareTo(key);
            if (cmp < 0 || !inclusive && cmp == 0) lo = mid + 1;
            else hi = mid;
        }

        return lo;
    }

    /// Index of the last child whose first key is lower than (or equal to, when inclusive) key, or -1.
    static int ChildBefore(Node* branch, ReadOnlySpan<byte> key, bool inclusive)
    {
        var entries = Branches(branch);
        int lo = 0, hi = branch->Count;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            var cmp = entries[mid].FirstKeySpan.SequenceCompareTo(key);
            if (cmp < 0 || inclusive && cmp == 0) lo = mid + 1;
            else hi = mid;
        }

        return lo - 1;
    }

    static int ChildFor(Node* branch, ReadOnlySpan<byte> key) => Math.Max(0, ChildBefore(branch, key, true));

    LeafEntry* Find(ReadOnlySpan<byte> key)
    {
        var node = _root;
        if (node == null) return null;
        while (node->IsLeaf == 0) node = Branches(node)[ChildFor(node, key)].Child;
        var index = LeafLowerBound(node, key, true);
        if (index < node->Count && Leaves(node)[index].KeySpan.SequenceEqual(key)) return &Leaves(node)[index];
        return null;
    }

    public bool TryGetNode(ReadOnlySpan<byte> key, out bool isSet, out bool existsBelow)
    {
        var entry = Find(key);
        isSet = entry != null && (entry->Flags & 1) != 0;
        existsBelow = entry != null && (entry->Flags & 2) != 0;
        return entry != null;
    }

    public void Insert(byte[] key, bool isSet, bool existsBelow, ReadOnlySpan<byte> value)
    {
        CheckWritable();
        var entry = new LeafEntry
        {
            Key = _arena.Copy(key), KeyLength = key.Length, Value = _arena.Copy(value), ValueLength = value.Length,
            Flags = (isSet ? 1 : 0) | (existsBelow ? 2 : 0)
        };
        if (_root == null) _root = NewNode(true);
        var node = InsertInto(_root, key, entry, out var split);
        if (split == null)
        {
            _root = node;
            return;
        }

        var root = NewNode(false);
        root->Count = 2;
        SetChild(root, 0, node);
        SetChild(root, 1, split);
        Recompute(root);
        _root = root;
    }

    Node* InsertInto(Node* node, ReadOnlySpan<byte> key, in LeafEntry entry, out Node* nodeSplit)
    {
        node = Own(node);
        if (node->IsLeaf != 0)
        {
            var position = LeafLowerBound(node, key, true);
            Node* split = null;
            var target = node;
            if (node->Count == Capacity)
            {
                split = NewNode(true);
                const int half = Capacity / 2;
                new Span<LeafEntry>(Leaves(node) + half, half).CopyTo(new(Leaves(split), half));
                node->Count = half;
                split->Count = half;
                if (position > half)
                {
                    target = split;
                    position -= half;
                }
            }

            var entries = Leaves(target);
            new Span<LeafEntry>(entries + position, target->Count - position)
                .CopyTo(new(entries + position + 1, target->Count - position));
            entries[position] = entry;
            target->Count++;
            Recompute(node);
            if (split != null) Recompute(split);
            nodeSplit = split;
            return node;
        }

        var index = ChildFor(node, key);
        var child = InsertInto(Branches(node)[index].Child, key, entry, out var childSplit);
        SetChild(node, index, child);
        if (childSplit == null)
        {
            Recompute(node);
            nodeSplit = null;
            return node;
        }

        Node* branchSplit = null;
        var targetBranch = node;
        var insertAt = index + 1;
        if (node->Count == Capacity)
        {
            branchSplit = NewNode(false);
            const int half = Capacity / 2;
            new Span<BranchEntry>(Branches(node) + half, half).CopyTo(new(Branches(branchSplit), half));
            node->Count = half;
            branchSplit->Count = half;
            if (insertAt > half)
            {
                targetBranch = branchSplit;
                insertAt -= half;
            }
        }

        var branches = Branches(targetBranch);
        new Span<BranchEntry>(branches + insertAt, targetBranch->Count - insertAt)
            .CopyTo(new(branches + insertAt + 1, targetBranch->Count - insertAt));
        targetBranch->Count++;
        SetChild(targetBranch, insertAt, childSplit);
        Recompute(node);
        if (branchSplit != null) Recompute(branchSplit);
        nodeSplit = branchSplit;
        return node;
    }

    public bool UpdateExisting(ReadOnlySpan<byte> key, bool isSet, ReadOnlySpan<byte> value)
    {
        CheckWritable();
        if (Find(key) == null) return false;
        var valueCopy = _arena.Copy(value);
        _root = UpdateIn(_root, key, isSet, valueCopy, value.Length);
        return true;
    }

    Node* UpdateIn(Node* node, ReadOnlySpan<byte> key, bool isSet, byte* value, int valueLength)
    {
        node = Own(node);
        if (node->IsLeaf != 0)
        {
            ref var entry = ref Leaves(node)[LeafLowerBound(node, key, true)];
            entry.Flags = (entry.Flags & 2) | (isSet ? 1 : 0);
            entry.Value = value;
            entry.ValueLength = valueLength;
        }
        else
        {
            var index = ChildFor(node, key);
            SetChild(node, index, UpdateIn(Branches(node)[index].Child, key, isSet, value, valueLength));
        }

        Recompute(node);
        return node;
    }

    public void Remove(ReadOnlySpan<byte> key)
    {
        CheckWritable();
        if (Find(key) == null) return;
        _root = Collapse(RemoveIn(_root, key, LsmKeys.Successor(key)));
    }

    public void RemoveRange(ReadOnlySpan<byte> start, ReadOnlySpan<byte> end)
    {
        CheckWritable();
        if (_root == null || LsmKeys.Compare(start, end) >= 0) return;
        _root = Collapse(RemoveIn(_root, start, end));
    }

    static Node* Collapse(Node* root)
    {
        while (root != null && root->IsLeaf == 0 && root->Count == 1) root = Branches(root)[0].Child;
        return root == null || root->Count == 0 ? null : root;
    }

    /// Removes the points in [start, end); returns the node, which may become empty.
    Node* RemoveIn(Node* node, ReadOnlySpan<byte> start, ReadOnlySpan<byte> end)
    {
        if (node->IsLeaf != 0)
        {
            var from = LeafLowerBound(node, start, true);
            var to = LeafLowerBound(node, end, true);
            if (from >= to) return node;
            node = Own(node);
            var entries = Leaves(node);
            new Span<LeafEntry>(entries + to, node->Count - to).CopyTo(new(entries + from, node->Count - to));
            node->Count -= to - from;
            Recompute(node);
            return node;
        }

        var first = Math.Max(0, ChildBefore(node, start, true));
        var last = ChildBefore(node, end, false);
        if (last < first) return node;
        node = Own(node);
        var branches = Branches(node);
        for (var i = first; i <= last; i++)
        {
            var child = RemoveIn(branches[i].Child, start, end);
            if (child->Count == 0) branches[i].Child = null;
            else SetChild(node, i, child);
        }

        var kept = 0;
        for (var i = 0; i < node->Count; i++)
        {
            if (branches[i].Child != null) branches[kept++] = branches[i];
        }

        node->Count = kept;
        Recompute(node);
        return node;
    }

    public void SetTombstones(TombstoneSet tombstones)
    {
        CheckWritable();
        _tombstones = tombstones;
    }

    public long PointDeltaBefore(ReadOnlySpan<byte> key)
    {
        long sum = 0;
        var node = _root;
        if (node == null) return 0;
        while (node->IsLeaf == 0)
        {
            var index = ChildBefore(node, key, false);
            if (index < 0) return sum;
            var branches = Branches(node);
            for (var i = 0; i < index; i++) sum += branches[i].DeltaSum;
            node = branches[index].Child;
        }

        var entries = Leaves(node);
        for (var i = 0; i < node->Count && entries[i].KeySpan.SequenceCompareTo(key) < 0; i++) sum += entries[i].Delta;
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
        if (node == null) return 0;
        while (node->IsLeaf == 0)
        {
            var index = ChildBefore(node, key, inclusive);
            if (index < 0) return count;
            var branches = Branches(node);
            for (var i = 0; i < index; i++) count += branches[i].Count;
            node = branches[index].Child;
        }

        return count + LeafLowerBound(node, key, !inclusive);
    }

    LeafEntry* EntryAt(long index)
    {
        var node = _root;
        while (node->IsLeaf == 0)
        {
            var branches = Branches(node);
            var i = 0;
            while (index >= branches[i].Count)
            {
                index -= branches[i].Count;
                i++;
            }

            node = branches[i].Child;
        }

        return &Leaves(node)[index];
    }

    public int BlockCount => (int)((EntryCount + VirtualBlockSize - 1) / VirtualBlockSize);

    public byte[] BlockFirstKey(int block) => EntryAt((long)block * VirtualBlockSize)->KeySpan.ToArray();

    public int CountBlocksBefore(ReadOnlySpan<byte> key, bool inclusive) =>
        (int)((CountBefore(key, inclusive) + VirtualBlockSize - 1) / VirtualBlockSize);

    /// Every FinePivotStride-th key of the virtual blocks; ranks in the memtable are exact, so this only bounds the
    /// final linear step of select.
    public void AddFinePivots(int fromBlock, int toBlock, List<byte[]> pivots)
    {
        var end = Math.Min((long)toBlock * VirtualBlockSize, EntryCount);
        for (var index = (long)Math.Max(0, fromBlock) * VirtualBlockSize; index < end; index += FinePivotStride)
            pivots.Add(EntryAt(index)->KeySpan.ToArray());
    }

    static LsmEntry ToEntry(LeafEntry* entry) => new(entry->KeySpan.ToArray(), (entry->Flags & 1) != 0,
        (entry->Flags & 2) != 0, new ReadOnlySpan<byte>(entry->Value, entry->ValueLength).ToArray());

    public bool TryGet(ReadOnlySpan<byte> key, out LsmEntry entry)
    {
        var found = Find(key);
        entry = found == null ? default : ToEntry(found);
        return found != null;
    }

    public bool SeekGE(ReadOnlySpan<byte> key, bool inclusive, out LsmEntry entry)
    {
        var found = SeekGE(_root, key, inclusive);
        entry = found == null ? default : ToEntry(found);
        return found != null;
    }

    static LeafEntry* SeekGE(Node* node, ReadOnlySpan<byte> key, bool inclusive)
    {
        if (node == null) return null;
        if (node->IsLeaf != 0)
        {
            var index = LeafLowerBound(node, key, inclusive);
            return index < node->Count ? &Leaves(node)[index] : null;
        }

        // The child that may hold key, then later children whose keys are all greater.
        for (var i = ChildFor(node, key); i < node->Count; i++)
        {
            var found = SeekGE(Branches(node)[i].Child, key, inclusive);
            if (found != null) return found;
        }

        return null;
    }

    public bool SeekLE(ReadOnlySpan<byte> key, bool inclusive, out LsmEntry entry)
    {
        var found = SeekLE(_root, key, inclusive);
        entry = found == null ? default : ToEntry(found);
        return found != null;
    }

    static LeafEntry* SeekLE(Node* node, ReadOnlySpan<byte> key, bool inclusive)
    {
        if (node == null) return null;
        if (node->IsLeaf != 0)
        {
            var index = LeafLowerBound(node, key, !inclusive) - 1;
            return index >= 0 ? &Leaves(node)[index] : null;
        }

        for (var i = ChildBefore(node, key, inclusive); i >= 0; i--)
        {
            var found = SeekLE(Branches(node)[i].Child, key, inclusive);
            if (found != null) return found;
        }

        return null;
    }

    public bool SeekLast(out LsmEntry entry)
    {
        var node = _root;
        if (node == null || node->Count == 0)
        {
            entry = default;
            return false;
        }

        while (node->IsLeaf == 0) node = Branches(node)[node->Count - 1].Child;
        entry = ToEntry(&Leaves(node)[node->Count - 1]);
        return true;
    }

    public IEnumerable<LsmEntry> Enumerate()
    {
        var root = RootAddress;
        if (root == 0) yield break;
        var stack = new Stack<(nint Node, int Index)>();
        stack.Push((root, 0));
        while (stack.Count > 0)
        {
            var (address, index) = stack.Pop();
            if (index >= NodeCount(address)) continue;
            stack.Push((address, index + 1));
            if (IsLeaf(address)) yield return EntryOf(address, index);
            else stack.Push((ChildOf(address, index), 0));
        }
    }

    nint RootAddress => (nint)_root;
    static int NodeCount(nint node) => ((Node*)node)->Count;
    static bool IsLeaf(nint node) => ((Node*)node)->IsLeaf != 0;
    static LsmEntry EntryOf(nint node, int index) => ToEntry(&Leaves((Node*)node)[index]);
    static nint ChildOf(nint node, int index) => (nint)Branches((Node*)node)[index].Child;

    public IComponentCursor CreateCursor() => new SeekingComponentCursor(this);
}
