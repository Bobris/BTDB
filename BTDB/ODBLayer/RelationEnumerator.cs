using System;
using System.Collections;
using System.Collections.Generic;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
// ReSharper disable MemberCanBeProtected.Global

namespace BTDB.ODBLayer
{
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
        public RelationEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo, ReadOnlySpan<byte> keyBytes,
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
    IRelationModificationCounter modificationCounter, int loaderIndex)
        {
            _transaction = tr;

            ItemLoader = relationInfo.ItemLoaderInfos[loaderIndex];
            _keyValueTr = _transaction.KeyValueDBTransaction;
            _prevProtectionCounter = _keyValueTr.CursorMovedCounter;

            KeyBytes = keyBytes;
            _modificationCounter = modificationCounter;
            _pos = 0;
            _seekNeeded = true;
            _prevModificationCounter = _modificationCounter.ModificationCounter;
        }

        public bool MoveNext()
        {
            if (!_seekNeeded)
                _pos++;
            if (_keyValueTr.CursorMovedCounter != _prevProtectionCounter)
            {
                _modificationCounter.CheckModifiedDuringEnum(_prevModificationCounter);
            }

            var ret = Seek();
            _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            return ret;
        }

        bool Seek()
        {
            if (!_keyValueTr.SetKeyIndex(KeyBytes, _pos))
                return false;
            _seekNeeded = false;
            return true;
        }

        public T Current
        {
            get
            {
                SeekCurrent();
                var keyBytes = _keyValueTr.GetKey();
                var valueBytes = _keyValueTr.GetValue();
                return CreateInstance(keyBytes, valueBytes);
            }
        }

        public ReadOnlySpan<byte> GetKeyBytes()
        {
            SeekCurrent();
            return _keyValueTr.GetKey();
        }

        void SeekCurrent()
        {
            if (_seekNeeded) throw new BTDBException("Invalid access to uninitialized Current.");
            if (_keyValueTr.CursorMovedCounter != _prevProtectionCounter)
            {
                _modificationCounter.CheckModifiedDuringEnum(_prevModificationCounter);
                Seek();
            }

            _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
        }

        protected virtual T CreateInstance(in ReadOnlySpan<byte> keyBytes, in ReadOnlySpan<byte> valueBytes)
        {
            return (T)ItemLoader.CreateInstance(_transaction, keyBytes, valueBytes);
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
            if (_pos > 0)
            {
                Reset();
            }

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
        readonly uint _fieldCountInKey;
        readonly IRelationDbManipulator _manipulator;

        public RelationSecondaryKeyEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
            in ReadOnlySpan<byte> keyBytes, uint secondaryKeyIndex, uint fieldCountInKey, IRelationDbManipulator manipulator,
            int loaderIndex)
            : base(tr, relationInfo, keyBytes, manipulator, loaderIndex)
        {
            _secondaryKeyIndex = secondaryKeyIndex;
            _fieldCountInKey = fieldCountInKey;
            _manipulator = manipulator;
        }

        protected override T CreateInstance(in ReadOnlySpan<byte> keyBytes, in ReadOnlySpan<byte> valueBytes)
        {
            return (T)_manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, _fieldCountInKey,
                KeyBytes, keyBytes.Slice(KeyBytes.Length));
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
            IRelationDbManipulator manipulator, in ReadOnlySpan<byte> prefixBytes, uint prefixFieldCount, int loaderIndex)
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

        internal static ReadOnlySpan<byte> FindLastKeyWithPrefix(in ReadOnlySpan<byte> endKeyBytes,
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
                var keyBytes = _keyValueTr.GetKey();
                return CreateInstance(keyBytes);
            }
        }

        protected virtual T CreateInstance(in ReadOnlySpan<byte> keyBytes)
        {
            return (T)ItemLoader.CreateInstance(_tr, keyBytes, _keyValueTr.GetValue());
        }

        public byte[] GetKeyBytes()
        {
            return _keyValueTr.GetKey().ToArray();
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
            if (_pos > 0)
            {
                Reset();
            }

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
            return (T)Manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, PrefixFieldCount,
                KeyBytes, keyBytes.Slice(KeyBytes.Length));
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
            return (TValue)ItemLoader.CreateInstance(_tr, keyBytes, _keyValueTr.GetValue());
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
            //read key
            var keyData = _keyValueTr.GetKey().Slice(KeyBytes.Length);
            var reader = new SpanReader(keyData);
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
                .GetSimpleLoader(new RelationInfo.SimpleLoaderType(advancedEnumParamField.Handler, typeof(TKey)));
        }

        protected override TValue CreateInstance(in ReadOnlySpan<byte> keyBytes)
        {
            return (TValue)Manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex,
                PrefixFieldCount, KeyBytes, keyBytes.Slice(KeyBytes.Length));
        }
    }
}
