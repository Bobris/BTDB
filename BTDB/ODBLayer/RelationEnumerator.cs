using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.Collections;

// ReSharper disable MemberCanBeProtected.Global

namespace BTDB.ODBLayer;

public struct ConstraintInfo
{
    public IConstraint Constraint;
    public IConstraint.MatchType MatchType;
    public int Offset;
}

class RelationConstraintEnumerator<T> : IEnumerator<T>, IEnumerable<T>
{
    readonly IInternalObjectDBTransaction _transaction;
    protected readonly RelationInfo.ItemLoaderInfo ItemLoader;
    readonly IRelationModificationCounter _modificationCounter;
    readonly ConstraintInfo[] _constraints;
    protected readonly IKeyValueDBTransaction KeyValueTr;
    readonly int _prevModificationCounter;
    long _prevProtectionCounter;

    long _pos;
    bool _seekNeeded;

    int _skipNextOn = -1;
    int _keyBytesCount;
    StructList<byte> _buffer;

    public RelationConstraintEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
        StructList<byte> keyBytes,
        IRelationModificationCounter modificationCounter, int loaderIndex, ConstraintInfo[] constraints)
    {
        _transaction = tr;

        ItemLoader = relationInfo.ItemLoaderInfos[loaderIndex];
        KeyValueTr = _transaction.KeyValueDBTransaction;
        _prevProtectionCounter = KeyValueTr.CursorMovedCounter;

        _buffer = keyBytes;
        _keyBytesCount = (int)_buffer.Count;

        _modificationCounter = modificationCounter;
        _constraints = constraints;
        _pos = 0;
        _seekNeeded = true;
        _prevModificationCounter = _modificationCounter.ModificationCounter;
        for (var i = 0; i < _constraints.Length; i++)
        {
            ref var c = ref constraints[i];
            c.MatchType = c.Constraint.Prepare(ref _buffer);
            c.Offset = -1;
        }
    }

    [SkipLocalsInit]
    public void GatherForSorting(ref SortNativeStorage sortNativeStorage, int[] ordererIdx, IOrderer[] orderers)
    {
        if (!KeyValueTr.FindFirstKey(_buffer.AsSpan(0, _keyBytesCount))) return;
        var skipNextOn = -1;
        sortNativeStorage.StartKeyIndex = (ulong)KeyValueTr.GetKeyIndex();
        Span<byte> writerBuf = stackalloc byte[512];
        var writer = new SpanWriter(writerBuf);
        Span<byte> buf = stackalloc byte[512];
        var i = 0;
        writer.WriteBlock(_buffer.AsReadOnlySpan(0, _keyBytesCount));
        while (i < _constraints.Length)
        {
            switch (_constraints[i].MatchType)
            {
                case IConstraint.MatchType.Exact:
                    _constraints[i].Constraint.WritePrefix(ref writer, _buffer);
                    _constraints[i].Offset = (int)writer.GetCurrentPosition();
                    break;
                case IConstraint.MatchType.Prefix:
                    _constraints[i].Constraint.WritePrefix(ref writer, _buffer);
                    goto case IConstraint.MatchType.NoPrefix;
                case IConstraint.MatchType.NoPrefix:
                    if (!KeyValueTr.FindFirstKey(writer.GetSpan())) return;
                    goto nextKeyTest;
                default:
                    throw new InvalidOperationException();
            }

            i++;
        }

        if (!KeyValueTr.FindFirstKey(writer.GetSpan())) return;
        goto nextKeyTest;
        goNextKey:
        if (!KeyValueTr.FindNextKey(new())) return;
        nextKeyTest:
        var key = KeyValueTr.GetKey(ref MemoryMarshal.GetReference(buf), buf.Length);
        var commonUpToOffset = PackUnpack.SequenceEqualUpTo(writer.GetSpan(), key);
        if (commonUpToOffset < _keyBytesCount) return;
        i = 0;
        while (i < _constraints.Length)
        {
            if (_constraints[i].Offset < 0) break;
            if (_constraints[i].Offset > commonUpToOffset) break;
            i++;
        }

        if (skipNextOn != -1 && i >= skipNextOn - 1)
        {
            goto goNextFast;
        }

        if (i != _constraints.Length)
        {
            goto prepareGoDown;
        }

        recordMatch:
        sortNativeStorage.StartNewItem();
        {
            var keyReader = new SpanReader(key);
            for (var j = 0; j < orderers.Length; j++)
            {
                var k = ordererIdx[j];
                var start = k > 0 ? (uint)_constraints[k - 1].Offset : (uint)_keyBytesCount;
                var end = (uint)_constraints[k].Offset;
                orderers[j].CopyOrderedField(key.Slice((int)start, (int)(end - start)), ref sortNativeStorage.Writer);
            }
        }

        sortNativeStorage.FinishNewItem((ulong)KeyValueTr.GetKeyIndex());
        writer.UpdateBuffer(key);
        goto goNextKey;
        prepareGoDown:
        for (var j = i; j < _constraints.Length; j++) _constraints[j].Offset = -1;
        var offsetPrefix = i > 0 ? _constraints[i - 1].Offset : _keyBytesCount;
        var reader = new SpanReader(key[offsetPrefix..]);
        if (i < skipNextOn) skipNextOn = -1;
        goDown:
        var matchResult = _constraints[i].Constraint.Match(ref reader, _buffer);
        _constraints[i].Offset = offsetPrefix + (int)reader.GetCurrentPositionWithoutController();

        if (matchResult == IConstraint.MatchResult.YesSkipNext)
        {
            i++;
            if (skipNextOn == -1) skipNextOn = i;
            if (i == _constraints.Length) goto recordMatch;
            goto goDown;
        }

        if (matchResult == IConstraint.MatchResult.Yes)
        {
            i++;
            if (i == _constraints.Length) goto recordMatch;
            goto goDown;
        }

        if (matchResult == IConstraint.MatchResult.NoAfterLast)
        {
            goto goNextFast;
        }

        switch (_constraints[i].MatchType)
        {
            case IConstraint.MatchType.NoPrefix:
            {
                goto goNextFast;
            }
            case IConstraint.MatchType.Prefix:
            {
                goto goNextFast;
            }
            case IConstraint.MatchType.Exact:
            {
                writer.Reset();
                writer.WriteBlock(key[..(i > 0 ? _constraints[i - 1].Offset : _keyBytesCount)]);
                do
                {
                    _constraints[i].Constraint.WritePrefix(ref writer, _buffer);
                    _constraints[i].Offset = (int)writer.GetCurrentPosition();
                    i++;
                    if (i != _constraints.Length) continue;
                    if (writer.GetSpan().SequenceCompareTo(key) < 0)
                    {
                        i--;
                        goto goNextFast;
                    }

                    KeyValueTr.FindExactOrNextKey(writer.GetSpan());
                    goto nextKeyTest;
                } while (_constraints[i].MatchType == IConstraint.MatchType.Exact);

                if (_constraints[i].MatchType == IConstraint.MatchType.Prefix)
                {
                    _constraints[i].Constraint.WritePrefix(ref writer, _buffer);
                    if (writer.GetSpan().SequenceCompareTo(key) < 0)
                    {
                        i--;
                        goto goNextFast;
                    }
                }
                else
                {
                    if (writer.GetSpan().SequenceCompareTo(key) < 0) goto goNextFast;
                }

                if (!KeyValueTr.FindExactOrNextKey(writer.GetSpan())) goto goNextFast;
                goto nextKeyTest;
            }
            default:
                throw new InvalidOperationException();
        }

        goNextFast:
        if (skipNextOn != -1)
        {
            i = skipNextOn - 2;
            skipNextOn = -1;
        }

        while (i >= 0)
        {
            switch (_constraints[i].MatchType)
            {
                case IConstraint.MatchType.Exact:
                    i--;
                    continue;
                default:
                {
                    var len = _constraints[i].Offset;
                    if (len < 0)
                    {
                        i--;
                        continue;
                    }

                    writer.Reset();
                    writer.WriteBlock(key[..len]);
                    if (!KeyValueTr.FindLastKey(writer.GetSpan())) throw new InvalidOperationException();
                    goto goNextKey;
                }
            }
        }
    }

    public bool MoveNextInGather()
    {
        bool ret;
        if (_seekNeeded)
        {
            _modificationCounter.CheckModifiedDuringEnum(_prevModificationCounter);
            ret = FindNextKey(true);
            _seekNeeded = false;
        }
        else
        {
            if (KeyValueTr.CursorMovedCounter != _prevProtectionCounter)
            {
                KeyValueTr.SetKeyIndex(_pos);
                _prevProtectionCounter = KeyValueTr.CursorMovedCounter;
            }

            ret = FindNextKey();
        }

        _prevProtectionCounter = KeyValueTr.CursorMovedCounter;
        _pos = KeyValueTr.GetKeyIndex();
        return ret;
    }

    public bool MoveNext()
    {
        bool ret;
        if (_seekNeeded)
        {
            _modificationCounter.CheckModifiedDuringEnum(_prevModificationCounter);
            ret = FindNextKey(true);
            _seekNeeded = false;
        }
        else
        {
            SeekCurrent();
            ret = FindNextKey();
        }

        _prevProtectionCounter = KeyValueTr.CursorMovedCounter;
        if (ret)
        {
            _pos = KeyValueTr.GetKeyIndex(_buffer.AsReadOnlySpan(0, _keyBytesCount));
        }

        return ret;
    }

    [SkipLocalsInit]
    bool FindNextKey(bool first = false)
    {
        Span<byte> writerBuf = stackalloc byte[512];
        var writer = new SpanWriter(writerBuf);
        Span<byte> buf = stackalloc byte[512];
        var i = 0;
        if (first)
        {
            writer.WriteBlock(_buffer.AsReadOnlySpan(0, _keyBytesCount));
            while (i < _constraints.Length)
            {
                switch (_constraints[i].MatchType)
                {
                    case IConstraint.MatchType.Exact:
                        _constraints[i].Constraint.WritePrefix(ref writer, _buffer);
                        _constraints[i].Offset = (int)writer.GetCurrentPosition();
                        break;
                    case IConstraint.MatchType.Prefix:
                        _constraints[i].Constraint.WritePrefix(ref writer, _buffer);
                        goto case IConstraint.MatchType.NoPrefix;
                    case IConstraint.MatchType.NoPrefix:
                        if (!KeyValueTr.FindFirstKey(writer.GetSpan())) return false;
                        goto nextKeyTest;
                    default:
                        throw new InvalidOperationException();
                }

                i++;
            }

            return KeyValueTr.FindFirstKey(writer.GetSpan());
        }

        writer.UpdateBuffer(KeyValueTr.GetKey(ref MemoryMarshal.GetReference(writerBuf), writerBuf.Length));
        goNextKey:
        if (!KeyValueTr.FindNextKey(new())) return false;
        nextKeyTest:
        var key = KeyValueTr.GetKey(ref MemoryMarshal.GetReference(buf), buf.Length);
        var commonUpToOffset = PackUnpack.SequenceEqualUpTo(writer.GetSpan(), key);
        if (commonUpToOffset < _keyBytesCount) return false;
        i = 0;
        while (i < _constraints.Length)
        {
            if (_constraints[i].Offset < 0) break;
            if (_constraints[i].Offset > commonUpToOffset) break;
            i++;
        }

        if (_skipNextOn != -1 && i >= _skipNextOn - 1)
        {
            goto goNextFast;
        }

        if (i == _constraints.Length) return true;
        for (var j = i; j < _constraints.Length; j++) _constraints[j].Offset = -1;
        var offsetPrefix = i > 0 ? _constraints[i - 1].Offset : _keyBytesCount;
        var reader = new SpanReader(key[offsetPrefix..]);
        if (i < _skipNextOn) _skipNextOn = -1;
        goDown:
        var matchResult = _constraints[i].Constraint.Match(ref reader, _buffer);
        _constraints[i].Offset = offsetPrefix + (int)reader.GetCurrentPositionWithoutController();

        if (matchResult == IConstraint.MatchResult.YesSkipNext)
        {
            i++;
            if (_skipNextOn == -1) _skipNextOn = i;
            if (i == _constraints.Length) return true;
            goto goDown;
        }

        if (matchResult == IConstraint.MatchResult.Yes)
        {
            i++;
            if (i == _constraints.Length) return true;
            goto goDown;
        }

        if (matchResult == IConstraint.MatchResult.NoAfterLast)
        {
            goto goNextFast;
        }

        switch (_constraints[i].MatchType)
        {
            case IConstraint.MatchType.NoPrefix:
            {
                goto goNextFast;
            }
            case IConstraint.MatchType.Prefix:
            {
                goto goNextFast;
            }
            case IConstraint.MatchType.Exact:
            {
                writer.Reset();
                writer.WriteBlock(key[..(i > 0 ? _constraints[i - 1].Offset : _keyBytesCount)]);
                do
                {
                    _constraints[i].Constraint.WritePrefix(ref writer, _buffer);
                    _constraints[i].Offset = (int)writer.GetCurrentPosition();
                    i++;
                    if (i != _constraints.Length) continue;
                    if (writer.GetSpan().SequenceCompareTo(key) < 0)
                    {
                        i--;
                        goto goNextFast;
                    }

                    if (KeyValueTr.FindExactOrNextKey(writer.GetSpan())) return true;
                    goto nextKeyTest;
                } while (_constraints[i].MatchType == IConstraint.MatchType.Exact);

                if (_constraints[i].MatchType == IConstraint.MatchType.Prefix)
                {
                    _constraints[i].Constraint.WritePrefix(ref writer, _buffer);
                    if (writer.GetSpan().SequenceCompareTo(key) < 0)
                    {
                        i--;
                        goto goNextFast;
                    }
                }
                else
                {
                    if (writer.GetSpan().SequenceCompareTo(key) < 0) goto goNextFast;
                }

                if (!KeyValueTr.FindExactOrNextKey(writer.GetSpan())) goto goNextFast;
                goto nextKeyTest;
            }
            default:
                throw new InvalidOperationException();
        }

        goNextFast:
        if (_skipNextOn != -1)
        {
            i = _skipNextOn - 2;
            _skipNextOn = -1;
        }

        while (i >= 0)
        {
            switch (_constraints[i].MatchType)
            {
                case IConstraint.MatchType.Exact:
                    i--;
                    continue;
                default:
                {
                    var len = _constraints[i].Offset;
                    if (len < 0)
                    {
                        i--;
                        continue;
                    }

                    writer.Reset();
                    writer.WriteBlock(key[..len]);
                    if (!KeyValueTr.FindLastKey(writer.GetSpan())) throw new InvalidOperationException();
                    goto goNextKey;
                }
            }
        }

        return false;
    }

    public virtual T Current
    {
        [SkipLocalsInit]
        get
        {
            SeekCurrent();
            Span<byte> keyBuffer = stackalloc byte[512];
            var keyBytes = KeyValueTr.GetKey(ref MemoryMarshal.GetReference(keyBuffer), keyBuffer.Length);
            return (T)ItemLoader.CreateInstance(_transaction, keyBytes);
        }
    }

    public virtual T CurrentInGather
    {
        [SkipLocalsInit]
        get
        {
            Span<byte> keyBuffer = stackalloc byte[512];
            var keyBytes = KeyValueTr.GetKey(ref MemoryMarshal.GetReference(keyBuffer), keyBuffer.Length);
            return (T)ItemLoader.CreateInstance(_transaction, keyBytes);
        }
    }

    protected void SeekCurrent()
    {
        if (_seekNeeded) throw new BTDBException("Invalid access to uninitialized Current.");
        if (KeyValueTr.CursorMovedCounter != _prevProtectionCounter)
        {
            _modificationCounter.CheckModifiedDuringEnum(_prevModificationCounter);
            KeyValueTr.SetKeyIndex(_buffer.AsReadOnlySpan(0, _keyBytesCount), _pos);
            _prevProtectionCounter = KeyValueTr.CursorMovedCounter;
        }
    }

    object IEnumerator.Current => Current!;

    public void Reset()
    {
        _pos = 0;
        _seekNeeded = true;
    }

    public void Dispose()
    {
    }

    public IEnumerator<T> GetEnumerator()
    {
        Reset();
        return this;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public virtual T CurrentByKeyIndex(ulong keyIndex)
    {
        KeyValueTr.SetKeyIndex((long)keyIndex);
        return CurrentInGather;
    }
}

class RelationConstraintSecondaryKeyEnumerator<T> : RelationConstraintEnumerator<T>
{
    readonly uint _secondaryKeyIndex;
    readonly IRelationDbManipulator _manipulator;

    public RelationConstraintSecondaryKeyEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
        StructList<byte> keyBytes, IRelationModificationCounter modificationCounter, int loaderIndex,
        ConstraintInfo[] constraints, uint secondaryKeyIndex, IRelationDbManipulator manipulator) : base(tr,
        relationInfo, keyBytes, modificationCounter, loaderIndex, constraints)
    {
        _secondaryKeyIndex = secondaryKeyIndex;
        _manipulator = manipulator;
    }

    public override T Current
    {
        [SkipLocalsInit]
        get
        {
            SeekCurrent();
            Span<byte> keyBuffer = stackalloc byte[512];
            var keyBytes = KeyValueTr.GetKey(ref MemoryMarshal.GetReference(keyBuffer), keyBuffer.Length);
            return (T)_manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, keyBytes);
        }
    }

    public override T CurrentInGather
    {
        [SkipLocalsInit]
        get
        {
            Span<byte> keyBuffer = stackalloc byte[512];
            var keyBytes = KeyValueTr.GetKey(ref MemoryMarshal.GetReference(keyBuffer), keyBuffer.Length);
            return (T)_manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, keyBytes);
        }
    }

    public override T CurrentByKeyIndex(ulong keyIndex)
    {
        KeyValueTr.SetKeyIndex((long)keyIndex);
        return CurrentInGather;
    }
}

class RelationEnumerator<T> : IEnumerator<T>, IEnumerable<T>
{
    readonly IInternalObjectDBTransaction _transaction;
    protected readonly RelationInfo.ItemLoaderInfo ItemLoader;
    readonly IRelationModificationCounter _modificationCounter;
    readonly IKeyValueDBTransaction _keyValueTr;
    long _prevProtectionCounter;

    uint _pos;
    bool _seekNeeded;

    protected readonly byte[] KeyBytes;
    readonly int _prevModificationCounter;

    public RelationEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
        ReadOnlySpan<byte> keyBytes,
        IRelationModificationCounter modificationCounter, int loaderIndex)
    {
        _transaction = tr;

        ItemLoader = relationInfo.ItemLoaderInfos[loaderIndex];
        _keyValueTr = _transaction.KeyValueDBTransaction;
        _prevProtectionCounter = _keyValueTr.CursorMovedCounter;

        KeyBytes = keyBytes.ToArray();
        _modificationCounter = modificationCounter;
        _pos = 0;
        _seekNeeded = true;
        _prevModificationCounter = _modificationCounter.ModificationCounter;
    }

    public RelationEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo, byte[] keyBytes,
        IRelationModificationCounter modificationCounter, int loaderIndex): this(tr, keyBytes, modificationCounter, relationInfo.ItemLoaderInfos[loaderIndex])
    {
    }

    public RelationEnumerator(IInternalObjectDBTransaction tr, byte[] keyBytes,
        IRelationModificationCounter modificationCounter, RelationInfo.ItemLoaderInfo loaderInfo)
    {
        _transaction = tr;

        ItemLoader = loaderInfo;
        _keyValueTr = _transaction.KeyValueDBTransaction;
        _prevProtectionCounter = _keyValueTr.CursorMovedCounter;

        KeyBytes = keyBytes;
        _modificationCounter = modificationCounter;
        _pos = 0;
        _seekNeeded = true;
        _prevModificationCounter = modificationCounter.ModificationCounter;
    }

    public bool MoveNext()
    {
        if (_seekNeeded)
        {
            _modificationCounter.CheckModifiedDuringEnum(_prevModificationCounter);
            var ret = _keyValueTr.FindFirstKey(KeyBytes);
            _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            _seekNeeded = false;
            return ret;
        }
        else
        {
            _pos++;
            if (_keyValueTr.CursorMovedCounter == _prevProtectionCounter)
            {
                var ret = _keyValueTr.FindNextKey(KeyBytes);
                _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
                return ret;
            }

            _modificationCounter.CheckModifiedDuringEnum(_prevModificationCounter);
            if (!_keyValueTr.SetKeyIndex(KeyBytes, _pos))
                return false;
            _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            return true;
        }
    }

    public T Current
    {
        [SkipLocalsInit]
        get
        {
            SeekCurrent();
            Span<byte> keyBuffer = stackalloc byte[512];
            var keyBytes = _keyValueTr.GetKey(ref MemoryMarshal.GetReference(keyBuffer), keyBuffer.Length);
            return CreateInstance(keyBytes);
        }
    }

    void SeekCurrent()
    {
        if (_seekNeeded) throw new BTDBException("Invalid access to uninitialized Current.");
        if (_keyValueTr.CursorMovedCounter != _prevProtectionCounter)
        {
            _modificationCounter.CheckModifiedDuringEnum(_prevModificationCounter);
            _keyValueTr.SetKeyIndex(KeyBytes, _pos);
            _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
        }
    }

    protected virtual T CreateInstance(in ReadOnlySpan<byte> keyBytes)
    {
        return (T)ItemLoader.CreateInstance(_transaction, keyBytes);
    }

    object IEnumerator.Current => Current!;

    public void Reset()
    {
        _pos = 0;
        _seekNeeded = true;
    }

    public void Dispose()
    {
    }

    public IEnumerator<T> GetEnumerator()
    {
        Reset();
        return this;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}

class RelationPrimaryKeyEnumerator<T> : RelationEnumerator<T>
{
    public RelationPrimaryKeyEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
        in ReadOnlySpan<byte> keyBytes, IRelationModificationCounter modificationCounter, int loaderIndex)
        : base(tr, relationInfo, keyBytes, modificationCounter, loaderIndex)
    {
    }
}

class RelationSecondaryKeyEnumerator<T> : RelationEnumerator<T>
{
    readonly uint _secondaryKeyIndex;
    readonly IRelationDbManipulator _manipulator;

    public RelationSecondaryKeyEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
        in ReadOnlySpan<byte> keyBytes, uint secondaryKeyIndex,
        IRelationDbManipulator manipulator,
        int loaderIndex)
        : base(tr, relationInfo, keyBytes, manipulator, loaderIndex)
    {
        _secondaryKeyIndex = secondaryKeyIndex;
        _manipulator = manipulator;
    }

    protected override T CreateInstance(in ReadOnlySpan<byte> keyBytes)
    {
        return (T)_manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, keyBytes);
    }
}

public class RelationAdvancedEnumerator<T> : IEnumerator<T>, IEnumerable<T>
{
    protected readonly uint PrefixFieldCount;
    protected readonly IRelationDbManipulator Manipulator;
    protected readonly RelationInfo.ItemLoaderInfo ItemLoader;
    readonly IInternalObjectDBTransaction _tr;
    readonly IKeyValueDBTransaction _keyValueTr;
    long _prevProtectionCounter;
    readonly uint _startPos;
    readonly uint _count;
    uint _pos;
    bool _seekNeeded;
    readonly bool _ascending;
    protected readonly byte[] KeyBytes;
    readonly int _prevModificationCounter;

    public RelationAdvancedEnumerator(
        IRelationDbManipulator manipulator,
        uint prefixFieldCount,
        EnumerationOrder order,
        KeyProposition startKeyProposition, int prefixLen, in ReadOnlySpan<byte> startKeyBytes,
        KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes, int loaderIndex)
    {
        PrefixFieldCount = prefixFieldCount;
        Manipulator = manipulator;
        ItemLoader = Manipulator.RelationInfo.ItemLoaderInfos[loaderIndex];

        _ascending = order == EnumerationOrder.Ascending;

        _tr = manipulator.Transaction;
        _keyValueTr = _tr.KeyValueDBTransaction;
        _prevProtectionCounter = _keyValueTr.CursorMovedCounter;

        KeyBytes = startKeyBytes.Slice(0, prefixLen).ToArray();
        var realEndKeyBytes = endKeyBytes;
        if (endKeyProposition == KeyProposition.Included)
            realEndKeyBytes = FindLastKeyWithPrefix(endKeyBytes, _keyValueTr);

        _keyValueTr.FindFirstKey(startKeyBytes.Slice(0, prefixLen));
        var prefixIndex = _keyValueTr.GetKeyIndex();

        if (prefixIndex == -1)
        {
            _count = 0;
            _startPos = 0;
            _pos = 0;
            _seekNeeded = true;
            return;
        }

        _prevModificationCounter = manipulator.ModificationCounter;

        long startIndex;
        long endIndex;
        if (endKeyProposition == KeyProposition.Ignored)
        {
            _keyValueTr.FindLastKey(startKeyBytes.Slice(0, prefixLen));
            endIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
        }
        else
        {
            switch (_keyValueTr.Find(realEndKeyBytes, (uint)prefixLen))
            {
                case FindResult.Exact:
                    endIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
                    if (endKeyProposition == KeyProposition.Excluded)
                    {
                        endIndex--;
                    }

                    break;
                case FindResult.Previous:
                    endIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
                    break;
                case FindResult.Next:
                    endIndex = _keyValueTr.GetKeyIndex() - prefixIndex - 1;
                    break;
                case FindResult.NotFound:
                    endIndex = -1;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        if (startKeyProposition == KeyProposition.Ignored)
        {
            startIndex = 0;
        }
        else
        {
            switch (_keyValueTr.Find(startKeyBytes, (uint)prefixLen))
            {
                case FindResult.Exact:
                    startIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
                    if (startKeyProposition == KeyProposition.Excluded)
                    {
                        startIndex++;
                    }

                    break;
                case FindResult.Previous:
                    startIndex = _keyValueTr.GetKeyIndex() - prefixIndex + 1;
                    break;
                case FindResult.Next:
                    startIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
                    break;
                case FindResult.NotFound:
                    startIndex = 0;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        _count = (uint)Math.Max(0, endIndex - startIndex + 1);
        _startPos = (uint)(_ascending ? startIndex : endIndex);
        _pos = 0;
        _seekNeeded = true;
    }

    public RelationAdvancedEnumerator(
        IRelationDbManipulator manipulator, in ReadOnlySpan<byte> prefixBytes, uint prefixFieldCount,
        int loaderIndex)
    {
        PrefixFieldCount = prefixFieldCount;
        Manipulator = manipulator;
        ItemLoader = Manipulator.RelationInfo.ItemLoaderInfos[loaderIndex];

        _ascending = true;

        _tr = manipulator.Transaction;
        _keyValueTr = _tr.KeyValueDBTransaction;
        _prevProtectionCounter = _keyValueTr.CursorMovedCounter;

        KeyBytes = prefixBytes.ToArray();

        _prevModificationCounter = manipulator.ModificationCounter;

        _count = (uint)_keyValueTr.GetKeyValueCount(prefixBytes);
        _startPos = _ascending ? 0 : _count - 1;
        _pos = 0;
        _seekNeeded = true;
    }

    internal static ReadOnlySpan<byte> FindLastKeyWithPrefix(scoped in ReadOnlySpan<byte> endKeyBytes,
        IKeyValueDBTransaction keyValueTr)
    {
        if (!keyValueTr.FindLastKey(endKeyBytes))
            return endKeyBytes;
        return keyValueTr.GetKey();
    }

    public bool MoveNext()
    {
        if (!_seekNeeded)
            _pos++;
        if (_pos >= _count)
            return false;
        if (_keyValueTr.CursorMovedCounter != _prevProtectionCounter)
        {
            Manipulator.CheckModifiedDuringEnum(_prevModificationCounter);
            Seek();
        }
        else if (_seekNeeded)
        {
            Seek();
        }
        else
        {
            if (_ascending)
            {
                _keyValueTr.FindNextKey(KeyBytes);
            }
            else
            {
                _keyValueTr.FindPreviousKey(KeyBytes);
            }
        }

        _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
        return true;
    }

    public void Reset()
    {
        _pos = 0;
        _seekNeeded = true;
    }

    public T Current
    {
        [SkipLocalsInit]
        get
        {
            if (_pos >= _count) throw new IndexOutOfRangeException();
            if (_seekNeeded) throw new BTDBException("Invalid access to uninitialized Current.");
            if (_keyValueTr.CursorMovedCounter != _prevProtectionCounter)
            {
                Manipulator.CheckModifiedDuringEnum(_prevModificationCounter);
                Seek();
            }

            _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            Span<byte> buffer = stackalloc byte[512];
            var keyBytes = _keyValueTr.GetKey(ref MemoryMarshal.GetReference(buffer), buffer.Length);
            return CreateInstance(keyBytes);
        }
    }

    protected virtual T CreateInstance(in ReadOnlySpan<byte> keyBytes)
    {
        return (T)ItemLoader.CreateInstance(_tr, keyBytes);
    }

    public byte[] GetKeyBytes()
    {
        return _keyValueTr.GetKeyToArray();
    }

    void Seek()
    {
        if (_ascending)
            _keyValueTr.SetKeyIndex(KeyBytes, _startPos + _pos);
        else
            _keyValueTr.SetKeyIndex(KeyBytes, _startPos - _pos);
        _seekNeeded = false;
    }

    object IEnumerator.Current => Current!;

    public void Dispose()
    {
    }

    public IEnumerator<T> GetEnumerator()
    {
        Reset();
        return this;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}

public class RelationAdvancedSecondaryKeyEnumerator<T> : RelationAdvancedEnumerator<T>
{
    readonly uint _secondaryKeyIndex;

    // ReSharper disable once UnusedMember.Global
    public RelationAdvancedSecondaryKeyEnumerator(
        IRelationDbManipulator manipulator,
        uint prefixFieldCount,
        EnumerationOrder order,
        KeyProposition startKeyProposition, int prefixLen, in ReadOnlySpan<byte> startKeyBytes,
        KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes,
        uint secondaryKeyIndex, int loaderIndex)
        : base(manipulator, prefixFieldCount, order,
            startKeyProposition, prefixLen, startKeyBytes,
            endKeyProposition, endKeyBytes, loaderIndex)
    {
        _secondaryKeyIndex = secondaryKeyIndex;
    }

    // ReSharper disable once UnusedMember.Global
    public RelationAdvancedSecondaryKeyEnumerator(
        IRelationDbManipulator manipulator,
        in ReadOnlySpan<byte> prefixBytes, uint prefixFieldCount,
        uint secondaryKeyIndex, int loaderIndex)
        : base(manipulator, prefixBytes, prefixFieldCount, loaderIndex)
    {
        _secondaryKeyIndex = secondaryKeyIndex;
    }

    protected override T CreateInstance(in ReadOnlySpan<byte> keyBytes)
    {
        return (T)Manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, keyBytes);
    }
}

public class RelationAdvancedOrderedEnumerator<TKey, TValue> : IOrderedDictionaryEnumerator<TKey, TValue>
{
    protected readonly uint PrefixFieldCount;
    protected readonly IRelationDbManipulator Manipulator;
    protected readonly RelationInfo.ItemLoaderInfo ItemLoader;
    readonly IInternalObjectDBTransaction _tr;
    readonly IKeyValueDBTransaction _keyValueTr;
    long _prevProtectionCounter;
    readonly uint _startPos;
    readonly uint _count;
    uint _pos;
    SeekState _seekState;
    readonly bool _ascending;
    protected readonly byte[] KeyBytes;
    protected ReaderFun<TKey>? KeyReader;

    public RelationAdvancedOrderedEnumerator(IRelationDbManipulator manipulator,
        uint prefixFieldCount, EnumerationOrder order,
        KeyProposition startKeyProposition, int prefixLen, in ReadOnlySpan<byte> startKeyBytes,
        KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes, bool initKeyReader, int loaderIndex)
    {
        PrefixFieldCount = prefixFieldCount;
        Manipulator = manipulator;
        ItemLoader = Manipulator.RelationInfo.ItemLoaderInfos[loaderIndex];

        _ascending = order == EnumerationOrder.Ascending;

        _tr = manipulator.Transaction;
        _keyValueTr = _tr.KeyValueDBTransaction;
        _prevProtectionCounter = _keyValueTr.CursorMovedCounter;

        KeyBytes = startKeyBytes.Slice(0, prefixLen).ToArray();
        var realEndKeyBytes = endKeyBytes;
        if (endKeyProposition == KeyProposition.Included)
            realEndKeyBytes = RelationAdvancedEnumerator<TValue>.FindLastKeyWithPrefix(endKeyBytes, _keyValueTr);

        _keyValueTr.FindFirstKey(startKeyBytes.Slice(0, prefixLen));
        var prefixIndex = _keyValueTr.GetKeyIndex();

        if (prefixIndex == -1)
        {
            _count = 0;
            _startPos = 0;
            _pos = 0;
            _seekState = SeekState.Undefined;
            return;
        }

        long startIndex;
        long endIndex;
        if (endKeyProposition == KeyProposition.Ignored)
        {
            _keyValueTr.FindLastKey(startKeyBytes.Slice(0, prefixLen));
            endIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
        }
        else
        {
            switch (_keyValueTr.Find(realEndKeyBytes, (uint)prefixLen))
            {
                case FindResult.Exact:
                    endIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
                    if (endKeyProposition == KeyProposition.Excluded)
                    {
                        endIndex--;
                    }

                    break;
                case FindResult.Previous:
                    endIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
                    break;
                case FindResult.Next:
                    endIndex = _keyValueTr.GetKeyIndex() - prefixIndex - 1;
                    break;
                case FindResult.NotFound:
                    endIndex = -1;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        if (startKeyProposition == KeyProposition.Ignored)
        {
            startIndex = 0;
        }
        else
        {
            switch (_keyValueTr.Find(startKeyBytes, (uint)prefixLen))
            {
                case FindResult.Exact:
                    startIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
                    if (startKeyProposition == KeyProposition.Excluded)
                    {
                        startIndex++;
                    }

                    break;
                case FindResult.Previous:
                    startIndex = _keyValueTr.GetKeyIndex() - prefixIndex + 1;
                    break;
                case FindResult.Next:
                    startIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
                    break;
                case FindResult.NotFound:
                    startIndex = 0;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        _count = (uint)Math.Max(0, endIndex - startIndex + 1);
        _startPos = (uint)(_ascending ? startIndex : endIndex);
        _pos = 0;
        _seekState = SeekState.Undefined;

        if (initKeyReader)
        {
            var primaryKeyFields = manipulator.RelationInfo.ClientRelationVersionInfo.PrimaryKeyFields;
            var advancedEnumParamField = primaryKeyFields.Span[(int)PrefixFieldCount];
            if (advancedEnumParamField.Handler!.NeedsCtx())
                throw new BTDBException("Not supported.");
            KeyReader = (ReaderFun<TKey>)manipulator.RelationInfo
                .GetSimpleLoader(new RelationInfo.SimpleLoaderType(advancedEnumParamField.Handler, typeof(TKey)));
        }
    }

    public uint Count => _count;

    protected virtual TValue CreateInstance(in ReadOnlySpan<byte> keyBytes)
    {
        return (TValue)ItemLoader.CreateInstance(_tr, keyBytes);
    }

    public TValue CurrentValue
    {
        get
        {
            if (_pos >= _count) throw new IndexOutOfRangeException();
            if (_seekState == SeekState.Undefined)
                throw new BTDBException("Invalid access to uninitialized CurrentValue.");
            if (_keyValueTr.CursorMovedCounter != _prevProtectionCounter)
            {
                Seek();
            }
            else if (_seekState != SeekState.Ready)
            {
                Seek();
            }

            _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            var keyBytes = _keyValueTr.GetKey();
            return CreateInstance(keyBytes);
        }
        set => throw new NotSupportedException();
    }

    void Seek()
    {
        if (_ascending)
            _keyValueTr.SetKeyIndex(KeyBytes, _startPos + _pos);
        else
            _keyValueTr.SetKeyIndex(KeyBytes, _startPos - _pos);
        _seekState = SeekState.Ready;
    }

    public uint Position
    {
        get => _pos;

        set
        {
            _pos = value > _count ? _count : value;
            _seekState = SeekState.SeekNeeded;
        }
    }

    [SkipLocalsInit]
    public bool NextKey(out TKey key)
    {
        if (_seekState == SeekState.Ready)
            _pos++;
        if (_pos >= _count)
        {
            key = default;
            return false;
        }

        if (_keyValueTr.CursorMovedCounter != _prevProtectionCounter)
        {
            Seek();
        }
        else if (_seekState != SeekState.Ready)
        {
            Seek();
        }
        else
        {
            if (_ascending)
            {
                _keyValueTr.FindNextKey(KeyBytes);
            }
            else
            {
                _keyValueTr.FindPreviousKey(KeyBytes);
            }
        }

        _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
        Span<byte> keyBuffer = stackalloc byte[512];
        var reader = new SpanReader(_keyValueTr.GetKey(ref MemoryMarshal.GetReference(keyBuffer), keyBuffer.Length)
            .Slice(KeyBytes.Length));
        key = KeyReader!(ref reader, null);
        return true;
    }
}

public class RelationAdvancedOrderedSecondaryKeyEnumerator<TKey, TValue> :
    RelationAdvancedOrderedEnumerator<TKey, TValue>
{
    readonly uint _secondaryKeyIndex;

    public RelationAdvancedOrderedSecondaryKeyEnumerator(IRelationDbManipulator manipulator,
        uint prefixFieldCount, EnumerationOrder order,
        KeyProposition startKeyProposition, int prefixLen, in ReadOnlySpan<byte> startKeyBytes,
        KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes,
        uint secondaryKeyIndex, int loaderIndex)
        : base(manipulator, prefixFieldCount, order,
            startKeyProposition, prefixLen, startKeyBytes,
            endKeyProposition, endKeyBytes, false, loaderIndex)
    {
        _secondaryKeyIndex = secondaryKeyIndex;
        var secKeyFields =
            manipulator.RelationInfo.ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex);
        var advancedEnumParamField = secKeyFields[(int)PrefixFieldCount];
        if (advancedEnumParamField.Handler!.NeedsCtx())
            throw new BTDBException("Not supported.");
        KeyReader = (ReaderFun<TKey>)manipulator.RelationInfo
            .GetSimpleLoader(new(advancedEnumParamField.Handler, typeof(TKey)));
    }

    protected override TValue CreateInstance(in ReadOnlySpan<byte> keyBytes)
    {
        return (TValue)Manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, keyBytes);
    }
}
