using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

class RelationConstraintReverseEnumerator<T> : RelationConstraintEnumerator<T> where T : class
{
    public RelationConstraintReverseEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
        in MemWriter keyBytes, in MemWriter keyBuffer, int loaderIndex, ConstraintInfo[] constraints)
        : base(tr, relationInfo, keyBytes, keyBuffer, loaderIndex, constraints)
    {
    }

    public new bool MoveNextInGather()
    {
        bool ret;
        if (_seekNeeded)
        {
            ret = FindPreviousKey(true);
            _seekNeeded = false;
        }
        else
        {
            ret = FindPreviousKey();
        }

        return ret;
    }

    public new bool MoveNext()
    {
        _transaction.ThrowIfDisposed();
        return MoveNextInGather();
    }

    [SkipLocalsInit]
    unsafe bool FindPreviousKey(bool first = false)
    {
        Span<byte> buf = stackalloc byte[1024];
        ref var writer = ref _key;
        var i = 0;
        if (first)
        {
            writer.Reset();
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
                        if (!_cursor!.FindLastKey(writer.GetSpan())) return false;
                        goto nextKeyTest;
                    default:
                        throw new InvalidOperationException();
                }

                i++;
            }

            if (_cursor!.FindLastKey(writer.GetSpan()))
            {
                _cursor.GetKeyIntoMemWriter(ref writer);
                return true;
            }

            return false;
        }

        goNextKey:
        if (!_cursor!.FindPreviousKey(new())) return false;
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

            if (matchResult == IConstraint.MatchResult.No)
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

                    if (writer.GetSpan().SequenceCompareTo(key) > 0)
                    {
                        i--;
                        goto goNextFast;
                    }

                    if (_cursor.FindLastKey(writer.GetSpan()))
                    {
                        _cursor.GetKeyIntoMemWriter(ref writer);
                        return true;
                    }

                    goto nextKeyTest;
                } while (_constraints[i].MatchType == IConstraint.MatchType.Exact);

                if (_constraints[i].MatchType == IConstraint.MatchType.Prefix)
                {
                    _constraints[i].Constraint.WritePrefix(ref writer, _buffer);
                    if (writer.GetSpan().SequenceCompareTo(key) > 0)
                    {
                        i--;
                        goto goNextFast;
                    }
                }
                else
                {
                    if (writer.GetSpan().SequenceCompareTo(key) > 0) goto goNextFast;
                }

                if (!_cursor.FindLastKey(writer.GetSpan())) goto goNextFast;
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
                    if (!_cursor.FindFirstKey(writer.GetSpan())) throw new InvalidOperationException();
                    goto goNextKey;
                }
            }
        }

        return false;
    }
}

class RelationConstraintSecondaryKeyReverseEnumerator<T> : RelationConstraintReverseEnumerator<T> where T : class
{
    readonly uint _secondaryKeyIndex;
    readonly IRelationDbManipulator _manipulator;

    public RelationConstraintSecondaryKeyReverseEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
        in MemWriter keyBytes, in MemWriter keyBuffer, int loaderIndex,
        ConstraintInfo[] constraints, uint secondaryKeyIndex, IRelationDbManipulator manipulator) : base(tr,
        relationInfo, keyBytes, keyBuffer, loaderIndex, constraints)
    {
        _secondaryKeyIndex = secondaryKeyIndex;
        _manipulator = manipulator;
    }

    public override T Current =>
        (T)_manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, _key.GetSpan())!;

    public override T CurrentFromKeySpan(ReadOnlySpan<byte> key)
    {
        return (T)_manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, key)!;
    }
}
