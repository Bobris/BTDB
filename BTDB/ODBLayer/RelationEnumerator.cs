using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

// ReSharper disable MemberCanBeProtected.Global

namespace BTDB.ODBLayer;

public struct ConstraintInfo
{
    public IConstraint Constraint;
    public IConstraint.MatchType MatchType;
    public int Offset;
}

class RelationConstraintEnumerator<T> : IEnumerator<T>, IEnumerable<T> where T : class
{
    readonly IInternalObjectDBTransaction _transaction;
    protected readonly RelationInfo.ItemLoaderInfo ItemLoader;
    readonly ConstraintInfo[] _constraints;
    protected readonly IKeyValueDBTransaction KeyValueTr;
    protected IKeyValueDBCursor _cursor;

    bool _seekNeeded;

    int _skipNextOn = -1;
    int _keyBytesCount;
    MemWriter _buffer;
    protected MemWriter _key;

    public RelationConstraintEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
        in MemWriter keyBytes, in MemWriter keyBuffer, int loaderIndex, ConstraintInfo[] constraints)
    {
        _transaction = tr;

        ItemLoader = relationInfo.ItemLoaderInfos[loaderIndex];
        KeyValueTr = _transaction.KeyValueDBTransaction;

        _key = keyBuffer;
        _cursor = KeyValueTr.CreateCursor();
        _buffer = keyBytes;
        _keyBytesCount = (int)_buffer.GetCurrentPosition();

        _constraints = constraints;
        _seekNeeded = true;
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
        if (!_cursor.FindFirstKey(_buffer.AsReadOnlySpan(0, _keyBytesCount))) return;
        sortNativeStorage.StartKeyIndex = (ulong)_cursor.GetKeyIndex();
        while (MoveNextInGather())
        {
            sortNativeStorage.StartNewItem();
            {
                for (var j = 0; j < orderers.Length; j++)
                {
                    var k = ordererIdx[j];
                    var start = k > 0 ? (uint)_constraints[k - 1].Offset : (uint)_keyBytesCount;
                    var end = (uint)_constraints[k].Offset;
                    var keyReader =
                        MemReader.CreateFromPinnedSpan(_key.GetSpan().Slice((int)start, (int)(end - start)));
                    orderers[j].CopyOrderedField(ref keyReader, ref sortNativeStorage.Writer);
                }
            }
            sortNativeStorage.FinishNewItem((ulong)_cursor.GetKeyIndex());
        }
    }

    public bool MoveNextInGather()
    {
        bool ret;
        if (_seekNeeded)
        {
            ret = FindNextKey(true);
            _seekNeeded = false;
        }
        else
        {
            ret = FindNextKey();
        }

        return ret;
    }

    public bool MoveNext()
    {
        _transaction.ThrowIfDisposed();
        return MoveNextInGather();
    }

    [SkipLocalsInit]
    unsafe bool FindNextKey(bool first = false)
    {
        Span<byte> buf = stackalloc byte[1024];
        ref var writer = ref _key;
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
                        if (!_cursor.FindFirstKey(writer.GetSpan())) return false;
                        goto nextKeyTest;
                    default:
                        throw new InvalidOperationException();
                }

                i++;
            }

            if (_cursor.FindFirstKey(writer.GetSpan()))
            {
                _cursor.GetKeyIntoMemWriter(ref writer);
                return true;
            }

            return false;
        }

        goNextKey:
        if (!_cursor.FindNextKey(new())) return false;
        nextKeyTest:
        var key = _cursor.GetKeySpan(ref buf);
        var commonUpToOffset = (uint)writer.GetSpan().CommonPrefixLength(key);
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

        if (i == _constraints.Length)
        {
            writer.UpdateBuffer(key);
            return true;
        }

        var dataDidntChangedForConstraint = i;
        for (var j = i; j < _constraints.Length; j++) _constraints[j].Offset = -1;
        var offsetPrefix = i > 0 ? _constraints[i - 1].Offset : _keyBytesCount;
        fixed (void* _ = key)
        {
            var reader = MemReader.CreateFromPinnedSpan(key[offsetPrefix..]);
            if (i < _skipNextOn) _skipNextOn = -1;
            goDown:
            var matchResult = _constraints[i].Constraint.Match(ref reader, _buffer);
            _constraints[i].Offset = offsetPrefix + (int)reader.GetCurrentPositionWithoutController();

            if (matchResult == IConstraint.MatchResult.YesSkipNext)
            {
                i++;
                if (_skipNextOn == -1) _skipNextOn = i;
                if (i == _constraints.Length)
                {
                    writer.UpdateBuffer(key);
                    return true;
                }

                goto goDown;
            }

            if (matchResult == IConstraint.MatchResult.Yes)
            {
                i++;
                if (i == _constraints.Length)
                {
                    writer.UpdateBuffer(key);
                    return true;
                }

                goto goDown;
            }

            if (matchResult == IConstraint.MatchResult.NoAfterLast)
            {
                goto goNextFast;
            }
        }

        if (dataDidntChangedForConstraint < i && _skipNextOn == -1)
        {
            goto goNextKey;
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

                    if (_cursor.FindExactOrNextKey(writer.GetSpan()))
                    {
                        _cursor.GetKeyIntoMemWriter(ref writer);
                        return true;
                    }

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

                if (!_cursor.FindExactOrNextKey(writer.GetSpan())) goto goNextFast;
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
                    if (!_cursor.FindLastKey(writer.GetSpan())) throw new InvalidOperationException();
                    goto goNextKey;
                }
            }
        }

        return false;
    }

    public virtual T Current
    {
        get => (T)ItemLoader.CreateInstance(_transaction, _cursor, _key.GetSpan());
    }

    public virtual T CurrentFromKeySpan(ReadOnlySpan<byte> key)
    {
        return (T)ItemLoader.CreateInstance(_transaction, _cursor, key);
    }

    object IEnumerator.Current => Current!;

    public void Reset()
    {
        _cursor.Invalidate();
        _seekNeeded = true;
    }

    public void Dispose()
    {
        _cursor.Dispose();
        _cursor = null!;
    }

    public IEnumerator<T> GetEnumerator()
    {
        return this;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public T CurrentByKeyIndex(ulong keyIndex)
    {
        _cursor.FindKeyIndex((long)keyIndex);
        _cursor.GetKeyIntoMemWriter(ref _key);
        return Current;
    }

    public unsafe ulong FastGather(long skip, long take, ICollection<T> target)
    {
        Span<byte> buf = stackalloc byte[1024];
        ref var writer = ref _key;
        var i = 0;
        var firstNonExact = 0;
        var offsetFirstNonExact = (uint)_keyBytesCount;
        writer.WriteBlock(_buffer.AsReadOnlySpan(0, _keyBytesCount));
        while (i < _constraints.Length)
        {
            switch (_constraints[i].MatchType)
            {
                case IConstraint.MatchType.Exact:
                    _constraints[i].Constraint.WritePrefix(ref writer, _buffer);
                    firstNonExact = i + 1;
                    offsetFirstNonExact = (uint)writer.GetCurrentPosition();
                    break;
                case IConstraint.MatchType.Prefix:
                    _constraints[i].Constraint.WritePrefix(ref writer, _buffer);
                    goto case IConstraint.MatchType.NoPrefix;
                case IConstraint.MatchType.NoPrefix:
                    if (!_cursor.FindFirstKey(writer.GetSpan())) return 0;
                    goto startIteration;
                default:
                    throw new InvalidOperationException();
            }

            i++;
        }

        startIteration:
        var count = 0UL;
        _cursor.FastIterate(ref buf, (index, key) =>
        {
            if (!key.StartsWith(_key.GetSpan())) return true;
            fixed (void* _ = key)
            {
                var keyReader = MemReader.CreateFromPinnedSpan(key);
                keyReader.SkipBlock(offsetFirstNonExact);
                i = firstNonExact;
                if (i < _constraints.Length)
                {
                    while (i + 1 < _constraints.Length)
                    {
                        if (!IConstraint.IsYesLike(_constraints[i].Constraint.Match(ref keyReader, _buffer)))
                            return false;
                        i++;
                    }

                    if (!IConstraint.IsYesLike(_constraints[i].Constraint.MatchLast(ref keyReader, _buffer)))
                        return false;
                }
            }

            if (skip > 0)
            {
                skip--;
            }
            else if (take > 0)
            {
                take--;
                target.Add(CurrentFromKeySpan(key));
            }

            count++;
            return false;
        });
        return count;
    }

    public ulong NormalGather(long skip, long take, ICollection<T> target)
    {
        var count = 0ul;

        while (MoveNextInGather())
        {
            count++;
            if (skip > 0)
            {
                skip--;
                continue;
            }

            if (take <= 0) continue;
            take--;
            target.Add(Current);
        }

        return count;
    }
}

class RelationConstraintSecondaryKeyEnumerator<T> : RelationConstraintEnumerator<T> where T : class
{
    readonly uint _secondaryKeyIndex;
    readonly IRelationDbManipulator _manipulator;

    public RelationConstraintSecondaryKeyEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
        in MemWriter keyBytes, in MemWriter keyBuffer, int loaderIndex,
        ConstraintInfo[] constraints, uint secondaryKeyIndex, IRelationDbManipulator manipulator) : base(tr,
        relationInfo, keyBytes, keyBuffer, loaderIndex, constraints)
    {
        _secondaryKeyIndex = secondaryKeyIndex;
        _manipulator = manipulator;
    }

    public override T Current =>
        (T)_manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, _key.GetSpan());

    public override T CurrentFromKeySpan(ReadOnlySpan<byte> key)
    {
        return (T)_manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, key);
    }
}

class RelationEnumerator<T> : IEnumerator<T>, IEnumerable<T>
{
    readonly IInternalObjectDBTransaction _transaction;
    protected readonly RelationInfo.ItemLoaderInfo ItemLoader;
    readonly IKeyValueDBTransaction _keyValueTr;

    protected IKeyValueDBCursor? _cursor;
    bool _seekNeeded;

    protected readonly byte[] KeyBytes;

    public RelationEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
        ReadOnlySpan<byte> keyBytes, int loaderIndex) : this(tr, keyBytes.ToArray(),
        relationInfo.ItemLoaderInfos[loaderIndex])
    {
    }

    public RelationEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo, byte[] keyBytes,
        int loaderIndex) : this(tr, keyBytes, relationInfo.ItemLoaderInfos[loaderIndex])
    {
    }

    public RelationEnumerator(IInternalObjectDBTransaction tr, byte[] keyBytes, RelationInfo.ItemLoaderInfo loaderInfo)
    {
        _transaction = tr;

        ItemLoader = loaderInfo;
        _keyValueTr = _transaction.KeyValueDBTransaction;

        KeyBytes = keyBytes;
        _seekNeeded = true;
    }

    public bool MoveNext()
    {
        ObjectDisposedException.ThrowIf(_cursor == null, this);
        _transaction.ThrowIfDisposed();
        if (_seekNeeded)
        {
            var ret = _cursor.FindFirstKey(KeyBytes);
            _seekNeeded = false;
            return ret;
        }

        return _cursor.FindNextKey(KeyBytes);
    }

    public T Current
    {
        [SkipLocalsInit]
        get
        {
            ObjectDisposedException.ThrowIf(_cursor == null, this);
            Span<byte> keyBuffer = stackalloc byte[2048];
            var keyBytes = _cursor.GetKeySpan(keyBuffer);
            return CreateInstance(keyBytes);
        }
    }

    protected virtual T CreateInstance(in ReadOnlySpan<byte> keyBytes)
    {
        return (T)ItemLoader.CreateInstance(_transaction, _cursor!, keyBytes);
    }

    object IEnumerator.Current => Current!;

    public void Reset()
    {
        _cursor ??= _keyValueTr.CreateCursor();
        _seekNeeded = true;
    }

    public void Dispose()
    {
        _cursor?.Dispose();
        _cursor = null;
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
        in ReadOnlySpan<byte> keyBytes, int loaderIndex)
        : base(tr, relationInfo, keyBytes, loaderIndex)
    {
    }

    public IKeyValueDBCursor Cursor => _cursor;
}

class RelationSecondaryKeyEnumerator<T> : RelationEnumerator<T>
{
    readonly uint _secondaryKeyIndex;
    readonly IRelationDbManipulator _manipulator;

    public RelationSecondaryKeyEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
        in ReadOnlySpan<byte> keyBytes, uint secondaryKeyIndex,
        IRelationDbManipulator manipulator,
        int loaderIndex)
        : base(tr, relationInfo, keyBytes, loaderIndex)
    {
        _secondaryKeyIndex = secondaryKeyIndex;
        _manipulator = manipulator;
    }

    protected override T CreateInstance(in ReadOnlySpan<byte> keyBytes)
    {
        return (T)_manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, keyBytes);
    }
}

public class RelationAdvancedEnumerator<T> : IEnumerator<T>, ICollection<T>
{
    protected readonly IRelationDbManipulator Manipulator;
    protected readonly RelationInfo.ItemLoaderInfo ItemLoader;
    readonly IInternalObjectDBTransaction _tr;
    readonly IKeyValueDBTransaction _keyValueTr;
    IKeyValueDBCursor? _startCursor;
    IKeyValueDBCursor? _endCursor;
    IKeyValueDBCursor? _cursor;
    bool _seekNeeded;
    readonly bool _ascending;
    readonly byte[] _keyBytes;
    readonly byte[]? _startKeyBytes;
    readonly byte[]? _endKeyBytes;
    readonly KeyProposition _startKeyProposition;
    readonly KeyProposition _endKeyProposition;

    public RelationAdvancedEnumerator(
        IRelationDbManipulator manipulator,
        EnumerationOrder order,
        KeyProposition startKeyProposition, int prefixLen, in ReadOnlySpan<byte> startKeyBytes,
        KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes, int loaderIndex)
    {
        Manipulator = manipulator;
        ItemLoader = Manipulator.RelationInfo.ItemLoaderInfos[loaderIndex];

        _ascending = order == EnumerationOrder.Ascending;

        _tr = manipulator.Transaction;
        _keyValueTr = _tr.KeyValueDBTransaction;

        _keyBytes = startKeyBytes.Slice(0, prefixLen).ToArray();
        _startKeyProposition = startKeyProposition;
        _endKeyProposition = endKeyProposition;
        if (_startKeyProposition != KeyProposition.Ignored)
        {
            _startKeyBytes = startKeyBytes.ToArray();
        }

        if (_endKeyProposition != KeyProposition.Ignored)
        {
            _endKeyBytes = endKeyBytes.ToArray();
        }
    }

    void CreateCursors()
    {
        _startCursor = _keyValueTr.CreateCursor();
        if (_startKeyProposition == KeyProposition.Ignored)
        {
            if (!_startCursor.FindFirstKey(_keyBytes))
            {
                return;
            }
        }
        else
        {
            switch (_startCursor.Find(_startKeyBytes, (uint)_keyBytes.Length))
            {
                case FindResult.Exact:
                    if (_startKeyProposition == KeyProposition.Excluded)
                    {
                        if (!_startCursor.FindNextKey(_keyBytes)) return;
                    }

                    break;
                case FindResult.Previous:
                    if (!_startCursor.FindNextKey(_keyBytes)) return;
                    if (_startKeyProposition == KeyProposition.Excluded)
                    {
                        if (!_startCursor.FindNextKey(_keyBytes)) return;
                    }

                    break;
                case FindResult.Next:
                    if (_startKeyProposition == KeyProposition.Excluded && _startCursor.KeyHasPrefix(_startKeyBytes))
                    {
                        if (!_startCursor.FindNextKey(_keyBytes)) return;
                    }

                    break;
                case FindResult.NotFound:
                    return;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        _endCursor = _keyValueTr.CreateCursor();
        var realEndKeyBytes = new ReadOnlySpan<byte>(_endKeyBytes);
        if (_endKeyProposition == KeyProposition.Included)
            realEndKeyBytes = FindLastKeyWithPrefix(_endKeyBytes.AsSpan(), _endCursor);

        if (_endKeyProposition == KeyProposition.Ignored)
        {
            if (!_endCursor.FindLastKey(_keyBytes)) return;
        }
        else
        {
            switch (_endCursor.Find(realEndKeyBytes, (uint)_keyBytes.Length))
            {
                case FindResult.Exact:
                    if (_endKeyProposition == KeyProposition.Excluded)
                    {
                        if (!_endCursor.FindPreviousKey(_keyBytes)) return;
                    }

                    break;
                case FindResult.Previous:
                    break;
                case FindResult.Next:
                    if (!_endCursor.FindPreviousKey(_keyBytes)) return;
                    break;
                case FindResult.NotFound:
                    return;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        var startIndex = _startCursor.GetKeyIndex();
        var endIndex = _endCursor.GetKeyIndex();
        if (startIndex > endIndex) return;
        _cursor = _keyValueTr.CreateCursor();
        _cursor.FindKeyIndex(_ascending ? startIndex : endIndex);
        _seekNeeded = true;
    }

    public RelationAdvancedEnumerator(
        IRelationDbManipulator manipulator, in ReadOnlySpan<byte> prefixBytes, int loaderIndex)
    {
        Manipulator = manipulator;
        ItemLoader = Manipulator.RelationInfo.ItemLoaderInfos[loaderIndex];

        _ascending = true;

        _tr = manipulator.Transaction;
        _keyValueTr = _tr.KeyValueDBTransaction;

        _keyBytes = prefixBytes.ToArray();
        _startKeyProposition = KeyProposition.Ignored;
        _endKeyProposition = KeyProposition.Ignored;
    }

    internal static ReadOnlySpan<byte> FindLastKeyWithPrefix(scoped in ReadOnlySpan<byte> endKeyBytes,
        IKeyValueDBCursor cursor)
    {
        if (!cursor.FindLastKey(endKeyBytes))
            return endKeyBytes;
        return cursor.GetKeySpan([]);
    }

    public bool MoveNext()
    {
        _tr.ThrowIfDisposed();
        if (_cursor == null)
        {
            return false;
        }

        if (_seekNeeded)
        {
            _seekNeeded = false;
        }
        else
        {
            if (_ascending)
            {
                if (!_cursor.FindNextKey(_keyBytes))
                {
                    return false;
                }

                if (_cursor.GetKeyIndex() > _endCursor!.GetKeyIndex())
                {
                    return false;
                }
            }
            else
            {
                if (!_cursor.FindPreviousKey(_keyBytes))
                {
                    return false;
                }

                if (_cursor.GetKeyIndex() < _startCursor!.GetKeyIndex())
                {
                    return false;
                }
            }
        }

        return true;
    }

    public void Reset()
    {
        Dispose();
        CreateCursors();
        _seekNeeded = true;
    }

    public T Current
    {
        [SkipLocalsInit]
        get
        {
            if (_seekNeeded) throw new BTDBException("Invalid access to uninitialized Current.");
            Span<byte> buffer = stackalloc byte[1024];
            var keyBytes = _cursor!.GetKeySpan(buffer);
            return CreateInstance(_cursor, keyBytes);
        }
    }

    protected virtual T CreateInstance(IKeyValueDBCursor cursor, in ReadOnlySpan<byte> keyBytes)
    {
        return (T)ItemLoader.CreateInstance(_tr, cursor, keyBytes);
    }

    public byte[] GetKeyBytes()
    {
        return _cursor!.SlowGetKey();
    }

    object IEnumerator.Current => Current!;

    public void Dispose()
    {
        _startCursor?.Dispose();
        _startCursor = null;
        _endCursor?.Dispose();
        _endCursor = null;
        _cursor?.Dispose();
        _cursor = null;
    }

    public IEnumerator<T> GetEnumerator()
    {
        CreateCursors();
        return this;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public void Add(T item)
    {
        throw new NotSupportedException();
    }

    public void Clear()
    {
        throw new NotSupportedException();
    }

    public bool Contains(T item)
    {
        throw new NotSupportedException();
    }

    public void CopyTo(T[] array, int arrayIndex)
    {
        CreateCursors();
        try
        {
            var count = _endCursor == null ? 0 : (int)(_endCursor.GetKeyIndex() - _startCursor!.GetKeyIndex() + 1);
            if (count == 0) return;
            if (array.Length - arrayIndex < count) throw new ArgumentException("Array too small");
            while (MoveNext())
            {
                array[arrayIndex++] = Current;
            }
        }
        finally
        {
            Dispose();
        }
    }

    public bool Remove(T item)
    {
        throw new NotSupportedException();
    }

    public int Count
    {
        get
        {
            try
            {
                CreateCursors();
                return _endCursor == null ? 0 : (int)(_endCursor.GetKeyIndex() - _startCursor!.GetKeyIndex() + 1);
            }
            finally
            {
                Dispose();
            }
        }
    }

    public bool IsReadOnly => true;

    public IKeyValueDBCursor Cursor => _cursor!;
}

public class RelationAdvancedSecondaryKeyEnumerator<T> : RelationAdvancedEnumerator<T>
{
    readonly uint _secondaryKeyIndex;

    // ReSharper disable once UnusedMember.Global
    public RelationAdvancedSecondaryKeyEnumerator(
        IRelationDbManipulator manipulator,
        EnumerationOrder order,
        KeyProposition startKeyProposition, int prefixLen, in ReadOnlySpan<byte> startKeyBytes,
        KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes,
        uint secondaryKeyIndex, int loaderIndex)
        : base(manipulator, order,
            startKeyProposition, prefixLen, startKeyBytes,
            endKeyProposition, endKeyBytes, loaderIndex)
    {
        _secondaryKeyIndex = secondaryKeyIndex;
    }

    // ReSharper disable once UnusedMember.Global
    public RelationAdvancedSecondaryKeyEnumerator(
        IRelationDbManipulator manipulator,
        in ReadOnlySpan<byte> prefixBytes,
        uint secondaryKeyIndex, int loaderIndex)
        : base(manipulator, prefixBytes, loaderIndex)
    {
        _secondaryKeyIndex = secondaryKeyIndex;
    }

    protected override T CreateInstance(IKeyValueDBCursor cursor, in ReadOnlySpan<byte> keyBytes)
    {
        return (T)Manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, keyBytes);
    }
}

public class RelationAdvancedOrderedEnumerator<TKey, TValue> : IOrderedDictionaryEnumerator<TKey, TValue>
{
    protected readonly IRelationDbManipulator Manipulator;
    protected readonly RelationInfo.ItemLoaderInfo ItemLoader;
    readonly IInternalObjectDBTransaction _tr;
    readonly IKeyValueDBTransaction _keyValueTr;
    IKeyValueDBCursor? _startCursor;
    IKeyValueDBCursor? _endCursor;
    IKeyValueDBCursor? _cursor;
    bool _seekNeeded;
    readonly bool _ascending;
    protected readonly byte[] KeyBytes;
    protected ReaderFun<TKey>? KeyReader;

    public RelationAdvancedOrderedEnumerator(IRelationDbManipulator manipulator, EnumerationOrder order,
        KeyProposition startKeyProposition, int prefixLen, in ReadOnlySpan<byte> startKeyBytes,
        KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes, int loaderIndex,
        int prefixFieldCount)
    {
        Manipulator = manipulator;
        ItemLoader = Manipulator.RelationInfo.ItemLoaderInfos[loaderIndex];

        _ascending = order == EnumerationOrder.Ascending;

        _tr = manipulator.Transaction;
        _keyValueTr = _tr.KeyValueDBTransaction;
        _seekNeeded = true;

        KeyBytes = startKeyBytes.Slice(0, prefixLen).ToArray();


        _startCursor = _keyValueTr.CreateCursor();
        if (startKeyProposition == KeyProposition.Ignored)
        {
            if (!_startCursor.FindFirstKey(KeyBytes))
            {
                return;
            }
        }
        else
        {
            switch (_startCursor.Find(startKeyBytes, (uint)prefixLen))
            {
                case FindResult.Exact:
                    if (startKeyProposition == KeyProposition.Excluded)
                    {
                        if (!_startCursor.FindNextKey(KeyBytes)) return;
                    }

                    break;
                case FindResult.Previous:
                    if (!_startCursor.FindNextKey(KeyBytes)) return;
                    break;
                case FindResult.Next:
                    break;
                case FindResult.NotFound:
                    return;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        _endCursor = _keyValueTr.CreateCursor();
        var realEndKeyBytes = endKeyBytes;
        if (endKeyProposition == KeyProposition.Included)
            realEndKeyBytes = RelationAdvancedEnumerator<TValue>.FindLastKeyWithPrefix(endKeyBytes, _endCursor);

        if (endKeyProposition == KeyProposition.Ignored)
        {
            if (!_endCursor.FindLastKey(KeyBytes)) return;
        }
        else
        {
            switch (_endCursor.Find(realEndKeyBytes, (uint)prefixLen))
            {
                case FindResult.Exact:
                    if (endKeyProposition == KeyProposition.Excluded)
                    {
                        if (!_endCursor.FindPreviousKey(KeyBytes)) return;
                    }

                    break;
                case FindResult.Previous:
                    break;
                case FindResult.Next:
                    if (!_endCursor.FindPreviousKey(KeyBytes)) return;
                    break;
                case FindResult.NotFound:
                    return;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        var startIndex = _startCursor.GetKeyIndex();
        var endIndex = _endCursor.GetKeyIndex();
        if (startIndex > endIndex) return;
        _cursor = _keyValueTr.CreateCursor();
        _cursor.FindKeyIndex(_ascending ? startIndex : endIndex);

        if (prefixFieldCount >= 0)
        {
            var primaryKeyFields = manipulator.RelationInfo.ClientRelationVersionInfo.PrimaryKeyFields;
            var advancedEnumParamField = primaryKeyFields.Span[prefixFieldCount];
            if (advancedEnumParamField.Handler!.NeedsCtx())
                throw new BTDBException("Not supported.");
            KeyReader = (ReaderFun<TKey>)manipulator.RelationInfo
                .GetSimpleLoader(new RelationInfo.SimpleLoaderType(advancedEnumParamField.Handler, typeof(TKey)));
        }
    }

    public uint Count => _cursor == null ? 0u : (uint)(_endCursor!.GetKeyIndex() - _startCursor!.GetKeyIndex() + 1);

    protected virtual TValue CreateInstance(IKeyValueDBCursor cursor, in ReadOnlySpan<byte> keyBytes)
    {
        return (TValue)ItemLoader.CreateInstance(_tr, cursor, keyBytes);
    }

    public TValue CurrentValue
    {
        [SkipLocalsInit]
        get
        {
            if (_seekNeeded) throw new BTDBException("Invalid access to uninitialized Current.");
            Span<byte> buffer = stackalloc byte[1024];
            var keyBytes = _cursor!.GetKeySpan(buffer);
            return CreateInstance(_cursor, keyBytes);
        }
        set => throw new NotSupportedException();
    }

    public int Position
    {
        get
        {
            if (_cursor == null) return 0;
            return (int)(_ascending
                ? _cursor.GetKeyIndex() - _startCursor!.GetKeyIndex()
                : _endCursor!.GetKeyIndex() - _cursor.GetKeyIndex());
        }

        set
        {
            if ((uint)value >= Count) throw new IndexOutOfRangeException();
            if (_ascending)
            {
                _cursor!.FindKeyIndex(_startCursor!.GetKeyIndex() + value);
            }
            else
            {
                _cursor!.FindKeyIndex(_endCursor!.GetKeyIndex() - value);
            }

            _seekNeeded = true;
        }
    }

    [SkipLocalsInit]
    public unsafe bool NextKey(out TKey key)
    {
        _tr.ThrowIfDisposed();
        if (_cursor == null)
        {
            key = default;
            return false;
        }

        if (_seekNeeded)
        {
            _seekNeeded = false;
        }
        else
        {
            if (_ascending)
            {
                if (!_cursor.FindNextKey(KeyBytes))
                {
                    key = default;
                    return false;
                }

                if (_cursor.GetKeyIndex() > _endCursor!.GetKeyIndex())
                {
                    key = default;
                    return false;
                }
            }
            else
            {
                if (!_cursor.FindPreviousKey(KeyBytes))
                {
                    key = default;
                    return false;
                }

                if (_cursor.GetKeyIndex() < _startCursor!.GetKeyIndex())
                {
                    key = default;
                    return false;
                }
            }
        }

        var keySpan = _cursor.GetKeySpan(stackalloc byte[2048])[KeyBytes.Length..];
        fixed (void* _ = keySpan)
        {
            var reader = MemReader.CreateFromPinnedSpan(keySpan);
            key = KeyReader!(ref reader, null);
        }

        return true;
    }

    public void Dispose()
    {
        _startCursor?.Dispose();
        _startCursor = null;
        _endCursor?.Dispose();
        _endCursor = null;
        _cursor?.Dispose();
        _cursor = null;
    }
}

public class RelationAdvancedOrderedSecondaryKeyEnumerator<TKey, TValue> :
    RelationAdvancedOrderedEnumerator<TKey, TValue>
{
    readonly uint _secondaryKeyIndex;

    public RelationAdvancedOrderedSecondaryKeyEnumerator(IRelationDbManipulator manipulator, EnumerationOrder order,
        KeyProposition startKeyProposition, int prefixLen, in ReadOnlySpan<byte> startKeyBytes,
        KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes,
        uint secondaryKeyIndex, int loaderIndex, int prefixFieldCount)
        : base(manipulator, order,
            startKeyProposition, prefixLen, startKeyBytes,
            endKeyProposition, endKeyBytes, loaderIndex, -1)
    {
        _secondaryKeyIndex = secondaryKeyIndex;
        var secKeyFields =
            manipulator.RelationInfo.ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex);
        var advancedEnumParamField = secKeyFields[prefixFieldCount];
        if (advancedEnumParamField.Handler!.NeedsCtx())
            throw new BTDBException("Not supported.");
        KeyReader = (ReaderFun<TKey>)manipulator.RelationInfo
            .GetSimpleLoader(new(advancedEnumParamField.Handler, typeof(TKey)));
    }

    protected override TValue CreateInstance(IKeyValueDBCursor cursor, in ReadOnlySpan<byte> keyBytes)
    {
        return (TValue)Manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, keyBytes);
    }
}
