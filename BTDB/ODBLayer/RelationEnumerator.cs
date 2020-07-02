using System;
using System.Collections;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    class RelationEnumerator<T> : IEnumerator<T>, IEnumerable<T>
    {
        protected readonly IInternalObjectDBTransaction Transaction;
        protected readonly RelationInfo RelationInfo;
        protected readonly RelationInfo.ItemLoaderInfo ItemLoader;
        readonly IRelationModificationCounter _modificationCounter;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector;
        readonly IKeyValueDBTransaction _keyValueTr;
        long _prevProtectionCounter;

        uint _pos;
        bool _seekNeeded;

        protected ByteBuffer KeyBytes;
        int _prevModificationCounter;
        public RelationEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo, ByteBuffer keyBytes,
            IRelationModificationCounter modificationCounter, int loaderIndex)
        {
            RelationInfo = relationInfo;
            Transaction = tr;

            ItemLoader = relationInfo.ItemLoaderInfos[loaderIndex];
            _keyValueTr = Transaction.KeyValueDBTransaction;
            _keyValueTrProtector = Transaction.TransactionProtector;
            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;

            KeyBytes = keyBytes;
            _modificationCounter = modificationCounter;
            _keyValueTrProtector.Start();
            _keyValueTr.SetKeyPrefix(KeyBytes);
            _pos = 0;
            _seekNeeded = true;
            _prevModificationCounter = _modificationCounter.ModificationCounter;
        }

        public bool MoveNext()
        {
            if (!_seekNeeded)
                _pos++;
            _keyValueTrProtector.Start();
            if (_keyValueTrProtector.WasInterupted(_prevProtectionCounter))
            {
                _modificationCounter.CheckModifiedDuringEnum(_prevModificationCounter);
                _keyValueTr.SetKeyPrefix(KeyBytes);
            }

            var ret = Seek();
            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
            return ret;
        }

        bool Seek()
        {
            if (!_keyValueTr.SetKeyIndex(_pos))
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

        public virtual ByteBuffer GetKeyBytes()
        {
            SeekCurrent();
            return _keyValueTr.GetKey();
        }

        void SeekCurrent()
        {
            if (_seekNeeded) throw new BTDBException("Invalid access to uninitialized Current.");
            _keyValueTrProtector.Start();
            if (_keyValueTrProtector.WasInterupted(_prevProtectionCounter))
            {
                _modificationCounter.CheckModifiedDuringEnum(_prevModificationCounter);
                _keyValueTr.SetKeyPrefix(KeyBytes);
                Seek();
            }

            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
        }

        protected virtual T CreateInstance(ByteBuffer keyBytes, ByteBuffer valueBytes)
        {
            return (T) ItemLoader.CreateInstance(Transaction, keyBytes, valueBytes);
        }

        object IEnumerator.Current => Current;

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
        readonly int _skipBytes;

        public RelationPrimaryKeyEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
            ByteBuffer keyBytes, IRelationModificationCounter modificationCounter, int loaderIndex)
            : base(tr, relationInfo, keyBytes, modificationCounter, loaderIndex)
        {
            _skipBytes = relationInfo.Prefix.Length;
        }

        protected override T CreateInstance(ByteBuffer keyBytes, ByteBuffer valueBytes)
        {
            var keyData = new byte[KeyBytes.Length - _skipBytes + keyBytes.Length];
            Array.Copy(KeyBytes.Buffer, KeyBytes.Offset + _skipBytes, keyData, 0, KeyBytes.Length - _skipBytes);
            Array.Copy(keyBytes.Buffer, keyBytes.Offset, keyData, KeyBytes.Length - _skipBytes, keyBytes.Length);

            return (T) ItemLoader.CreateInstance(Transaction, ByteBuffer.NewAsync(keyData), valueBytes);
        }

        public override ByteBuffer GetKeyBytes()
        {
            var keyBytes = base.GetKeyBytes();
            var keyData = new byte[KeyBytes.Length + keyBytes.Length];
            Array.Copy(KeyBytes.Buffer, KeyBytes.Offset, keyData, 0, KeyBytes.Length);
            Array.Copy(keyBytes.Buffer, keyBytes.Offset, keyData, KeyBytes.Length, keyBytes.Length);

            return ByteBuffer.NewAsync(keyData);
        }
    }

    class RelationSecondaryKeyEnumerator<T> : RelationEnumerator<T>
    {
        readonly uint _secondaryKeyIndex;
        readonly uint _fieldCountInKey;
        readonly IRelationDbManipulator _manipulator;

        public RelationSecondaryKeyEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
            ByteBuffer keyBytes, uint secondaryKeyIndex, uint fieldCountInKey, IRelationDbManipulator manipulator,
            int loaderIndex)
            : base(tr, relationInfo, keyBytes, manipulator.ModificationCounter, loaderIndex)
        {
            _secondaryKeyIndex = secondaryKeyIndex;
            _fieldCountInKey = fieldCountInKey;
            _manipulator = manipulator;
        }

        protected override T CreateInstance(ByteBuffer keyBytes, ByteBuffer valueBytes)
        {
            return (T) _manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, _fieldCountInKey,
                KeyBytes, keyBytes);
        }
    }

    public class RelationAdvancedEnumerator<T> : IEnumerator<T>, IEnumerable<T>
    {
        protected readonly uint _prefixFieldCount;
        protected readonly IRelationDbManipulator _manipulator;
        protected readonly RelationInfo.ItemLoaderInfo ItemLoader;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector;
        readonly IInternalObjectDBTransaction _tr;
        readonly IKeyValueDBTransaction _keyValueTr;
        long _prevProtectionCounter;
        readonly uint _startPos;
        readonly uint _count;
        uint _pos;
        bool _seekNeeded;
        readonly bool _ascending;
        protected readonly ByteBuffer _keyBytes;
        readonly int _lengthOfNonDataPrefix;
        readonly int _prevModificationCounter;

        public RelationAdvancedEnumerator(
            IRelationDbManipulator manipulator,
            ByteBuffer prefixBytes, uint prefixFieldCount,
            EnumerationOrder order,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes, int loaderIndex)
        {
            _prefixFieldCount = prefixFieldCount;
            _manipulator = manipulator;
            ItemLoader = _manipulator.RelationInfo.ItemLoaderInfos[loaderIndex];

            _ascending = order == EnumerationOrder.Ascending;

            _tr = manipulator.Transaction;
            _keyValueTr = _tr.KeyValueDBTransaction;
            _keyValueTrProtector = _tr.TransactionProtector;
            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;

            _keyBytes = prefixBytes;
            if (endKeyProposition == KeyProposition.Included)
                endKeyBytes = FindLastKeyWithPrefix(_keyBytes, endKeyBytes, _keyValueTr, _keyValueTrProtector);

            _keyValueTrProtector.Start();
            _keyValueTr.SetKeyPrefix(_keyBytes);

            _prevModificationCounter = manipulator.ModificationCounter.ModificationCounter;

            long startIndex;
            long endIndex;
            if (endKeyProposition == KeyProposition.Ignored)
            {
                endIndex = _keyValueTr.GetKeyValueCount() - 1;
            }
            else
            {
                switch (_keyValueTr.Find(endKeyBytes))
                {
                    case FindResult.Exact:
                        endIndex = _keyValueTr.GetKeyIndex();
                        if (endKeyProposition == KeyProposition.Excluded)
                        {
                            endIndex--;
                        }

                        break;
                    case FindResult.Previous:
                        endIndex = _keyValueTr.GetKeyIndex();
                        break;
                    case FindResult.Next:
                        endIndex = _keyValueTr.GetKeyIndex() - 1;
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
                switch (_keyValueTr.Find(startKeyBytes))
                {
                    case FindResult.Exact:
                        startIndex = _keyValueTr.GetKeyIndex();
                        if (startKeyProposition == KeyProposition.Excluded)
                        {
                            startIndex++;
                        }

                        break;
                    case FindResult.Previous:
                        startIndex = _keyValueTr.GetKeyIndex() + 1;
                        break;
                    case FindResult.Next:
                        startIndex = _keyValueTr.GetKeyIndex();
                        break;
                    case FindResult.NotFound:
                        startIndex = 0;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            _count = (uint) Math.Max(0, endIndex - startIndex + 1);
            _startPos = (uint) (_ascending ? startIndex : endIndex);
            _pos = 0;
            _seekNeeded = true;
            _lengthOfNonDataPrefix = manipulator.RelationInfo.Prefix.Length;
        }

        public RelationAdvancedEnumerator(
            IRelationDbManipulator manipulator, ByteBuffer prefixBytes, uint prefixFieldCount, int loaderIndex)
        {
            _prefixFieldCount = prefixFieldCount;
            _manipulator = manipulator;
            ItemLoader = _manipulator.RelationInfo.ItemLoaderInfos[loaderIndex];

            _ascending = true;

            _tr = manipulator.Transaction;
            _keyValueTr = _tr.KeyValueDBTransaction;
            _keyValueTrProtector = _tr.TransactionProtector;
            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;

            _keyBytes = prefixBytes;

            _keyValueTrProtector.Start();
            _keyValueTr.SetKeyPrefix(_keyBytes);

            _prevModificationCounter = manipulator.ModificationCounter.ModificationCounter;

            _count = (uint) _keyValueTr.GetKeyValueCount();
            _startPos = _ascending ? 0 : _count - 1;
            _pos = 0;
            _seekNeeded = true;
            _lengthOfNonDataPrefix = manipulator.RelationInfo.Prefix.Length;
        }

        internal static ByteBuffer FindLastKeyWithPrefix(ByteBuffer keyBytes, ByteBuffer endKeyBytes,
            IKeyValueDBTransaction keyValueTr, KeyValueDBTransactionProtector keyValueTrProtector)
        {
            var buffer = ByteBuffer.NewEmpty();
            buffer = buffer.ResizingAppend(keyBytes).ResizingAppend(endKeyBytes);
            keyValueTrProtector.Start();
            keyValueTr.SetKeyPrefix(buffer);
            if (!keyValueTr.FindLastKey())
                return endKeyBytes;
            var key = keyValueTr.GetKeyIncludingPrefix();
            return key.Slice(keyBytes.Length);
        }

        public bool MoveNext()
        {
            if (!_seekNeeded)
                _pos++;
            if (_pos >= _count)
                return false;
            _keyValueTrProtector.Start();
            if (_keyValueTrProtector.WasInterupted(_prevProtectionCounter))
            {
                _manipulator.ModificationCounter.CheckModifiedDuringEnum(_prevModificationCounter);
                _keyValueTr.SetKeyPrefix(_keyBytes);
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
                    _keyValueTr.FindNextKey();
                }
                else
                {
                    _keyValueTr.FindPreviousKey();
                }
            }

            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
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
                _keyValueTrProtector.Start();
                if (_keyValueTrProtector.WasInterupted(_prevProtectionCounter))
                {
                    _manipulator.ModificationCounter.CheckModifiedDuringEnum(_prevModificationCounter);
                    _keyValueTr.SetKeyPrefix(_keyBytes);
                    Seek();
                }

                _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                var keyBytes = _keyValueTr.GetKey();
                return CreateInstance(keyBytes);
            }
        }

        protected virtual T CreateInstance(ByteBuffer keyBytes)
        {
            var data = new byte[_keyBytes.Length - _lengthOfNonDataPrefix + keyBytes.Length];
            Array.Copy(_keyBytes.Buffer, _keyBytes.Offset + _lengthOfNonDataPrefix, data, 0,
                _keyBytes.Length - _lengthOfNonDataPrefix);
            Array.Copy(keyBytes.Buffer, keyBytes.Offset, data, _keyBytes.Length - _lengthOfNonDataPrefix,
                keyBytes.Length);

            return (T) ItemLoader.CreateInstance(_tr, ByteBuffer.NewAsync(data), _keyValueTr.GetValue());
        }

        public ByteBuffer GetKeyBytes()
        {
            return _keyValueTr.GetKeyIncludingPrefix();
        }

        void Seek()
        {
            if (_ascending)
                _keyValueTr.SetKeyIndex(_startPos + _pos);
            else
                _keyValueTr.SetKeyIndex(_startPos - _pos);
            _seekNeeded = false;
        }

        object IEnumerator.Current => Current;

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

        public RelationAdvancedSecondaryKeyEnumerator(
            IRelationDbManipulator manipulator,
            ByteBuffer prefixBytes, uint prefixFieldCount,
            EnumerationOrder order,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes,
            uint secondaryKeyIndex, int loaderIndex)
            : base(manipulator, prefixBytes, prefixFieldCount, order,
                startKeyProposition, startKeyBytes,
                endKeyProposition, endKeyBytes, loaderIndex)
        {
            _secondaryKeyIndex = secondaryKeyIndex;
        }

        public RelationAdvancedSecondaryKeyEnumerator(
            IRelationDbManipulator manipulator,
            ByteBuffer prefixBytes, uint prefixFieldCount,
            uint secondaryKeyIndex, int loaderIndex)
            : base(manipulator, prefixBytes, prefixFieldCount, loaderIndex)
        {
            _secondaryKeyIndex = secondaryKeyIndex;
        }

        protected override T CreateInstance(ByteBuffer keyBytes)
        {
            return (T) _manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex, _prefixFieldCount,
                _keyBytes, keyBytes);
        }
    }

    public class RelationAdvancedOrderedEnumerator<TKey, TValue> : IOrderedDictionaryEnumerator<TKey, TValue>
    {
        protected readonly uint _prefixFieldCount;
        protected readonly IRelationDbManipulator _manipulator;
        protected readonly RelationInfo.ItemLoaderInfo ItemLoader;
        readonly IInternalObjectDBTransaction _tr;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector;
        readonly IKeyValueDBTransaction _keyValueTr;
        long _prevProtectionCounter;
        readonly uint _startPos;
        readonly uint _count;
        uint _pos;
        SeekState _seekState;
        readonly bool _ascending;
        protected readonly ByteBuffer _keyBytes;
        protected ReaderFun<TKey> _keyReader;
        readonly int _lengthOfNonDataPrefix;

        public RelationAdvancedOrderedEnumerator(IRelationDbManipulator manipulator,
            ByteBuffer prefixBytes, uint prefixFieldCount,
            EnumerationOrder order,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes, bool initKeyReader, int loaderIndex)
        {
            _prefixFieldCount = prefixFieldCount;
            _manipulator = manipulator;
            ItemLoader = _manipulator.RelationInfo.ItemLoaderInfos[loaderIndex];
            _tr = manipulator.Transaction;
            _ascending = order == EnumerationOrder.Ascending;

            _keyValueTr = _tr.KeyValueDBTransaction;
            _keyValueTrProtector = _tr.TransactionProtector;
            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;

            _keyBytes = prefixBytes;
            if (endKeyProposition == KeyProposition.Included)
                endKeyBytes = RelationAdvancedEnumerator<TValue>.FindLastKeyWithPrefix(_keyBytes, endKeyBytes,
                    _keyValueTr, _keyValueTrProtector);

            _keyValueTrProtector.Start();
            _keyValueTr.SetKeyPrefix(_keyBytes);

            long startIndex;
            long endIndex;
            if (endKeyProposition == KeyProposition.Ignored)
            {
                endIndex = _keyValueTr.GetKeyValueCount() - 1;
            }
            else
            {
                switch (_keyValueTr.Find(endKeyBytes))
                {
                    case FindResult.Exact:
                        endIndex = _keyValueTr.GetKeyIndex();
                        if (endKeyProposition == KeyProposition.Excluded)
                        {
                            endIndex--;
                        }

                        break;
                    case FindResult.Previous:
                        endIndex = _keyValueTr.GetKeyIndex();
                        break;
                    case FindResult.Next:
                        endIndex = _keyValueTr.GetKeyIndex() - 1;
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
                switch (_keyValueTr.Find(startKeyBytes))
                {
                    case FindResult.Exact:
                        startIndex = _keyValueTr.GetKeyIndex();
                        if (startKeyProposition == KeyProposition.Excluded)
                        {
                            startIndex++;
                        }

                        break;
                    case FindResult.Previous:
                        startIndex = _keyValueTr.GetKeyIndex() + 1;
                        break;
                    case FindResult.Next:
                        startIndex = _keyValueTr.GetKeyIndex();
                        break;
                    case FindResult.NotFound:
                        startIndex = 0;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            _count = (uint) Math.Max(0, endIndex - startIndex + 1);
            _startPos = (uint) (_ascending ? startIndex : endIndex);
            _pos = 0;
            _seekState = SeekState.Undefined;

            if (initKeyReader)
            {
                var primaryKeyFields = manipulator.RelationInfo.ClientRelationVersionInfo.PrimaryKeyFields;
                var advancedEnumParamField = primaryKeyFields.Span[(int) _prefixFieldCount];
                if (advancedEnumParamField.Handler!.NeedsCtx())
                    throw new BTDBException("Not supported.");
                _keyReader = (ReaderFun<TKey>) manipulator.RelationInfo
                    .GetSimpleLoader(new RelationInfo.SimpleLoaderType(advancedEnumParamField.Handler, typeof(TKey)));

                _lengthOfNonDataPrefix = manipulator.RelationInfo.Prefix.Length;
            }
        }

        public uint Count => _count;

        protected virtual TValue CreateInstance(ByteBuffer prefixKeyBytes, ByteBuffer keyBytes)
        {
            var data = new byte[_keyBytes.Length - _lengthOfNonDataPrefix + keyBytes.Length];
            Array.Copy(_keyBytes.Buffer!, _keyBytes.Offset + _lengthOfNonDataPrefix, data, 0,
                _keyBytes.Length - _lengthOfNonDataPrefix);
            Array.Copy(keyBytes.Buffer!, keyBytes.Offset, data, _keyBytes.Length - _lengthOfNonDataPrefix,
                keyBytes.Length);

            return (TValue) ItemLoader.CreateInstance(_tr, ByteBuffer.NewAsync(data),
                _keyValueTr.GetValue());
        }

        public TValue CurrentValue
        {
            get
            {
                if (_pos >= _count) throw new IndexOutOfRangeException();
                if (_seekState == SeekState.Undefined)
                    throw new BTDBException("Invalid access to uninitialized CurrentValue.");
                _keyValueTrProtector.Start();
                if (_keyValueTrProtector.WasInterupted(_prevProtectionCounter))
                {
                    _keyValueTr.SetKeyPrefix(_keyBytes);
                    Seek();
                }
                else if (_seekState != SeekState.Ready)
                {
                    Seek();
                }

                _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                var keyBytes = _keyValueTr.GetKey();
                return CreateInstance(_keyBytes, keyBytes);
            }
            set => throw new NotSupportedException();
        }

        void Seek()
        {
            if (_ascending)
                _keyValueTr.SetKeyIndex(_startPos + _pos);
            else
                _keyValueTr.SetKeyIndex(_startPos - _pos);
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
                key = default(TKey);
                return false;
            }

            _keyValueTrProtector.Start();
            if (_keyValueTrProtector.WasInterupted(_prevProtectionCounter))
            {
                _keyValueTr.SetKeyPrefix(_keyBytes);
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
                    _keyValueTr.FindNextKey();
                }
                else
                {
                    _keyValueTr.FindPreviousKey();
                }
            }

            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
            //read key
            var keyData = _keyValueTr.GetKey().AsSyncReadOnlySpan();
            var reader = new SpanReader(keyData);
            key = _keyReader(ref reader, null);
            return true;
        }
    }

    public class RelationAdvancedOrderedSecondaryKeyEnumerator<TKey, TValue> :
        RelationAdvancedOrderedEnumerator<TKey, TValue>
    {
        readonly uint _secondaryKeyIndex;

        public RelationAdvancedOrderedSecondaryKeyEnumerator(IRelationDbManipulator manipulator,
            ByteBuffer prefixBytes, uint prefixFieldCount, EnumerationOrder order,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes,
            uint secondaryKeyIndex, int loaderIndex)
            : base(manipulator, prefixBytes, prefixFieldCount, order,
                startKeyProposition, startKeyBytes,
                endKeyProposition, endKeyBytes, false, loaderIndex)
        {
            _secondaryKeyIndex = secondaryKeyIndex;
            var secKeyFields =
                manipulator.RelationInfo.ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex);
            var advancedEnumParamField = secKeyFields[(int) _prefixFieldCount];
            if (advancedEnumParamField.Handler!.NeedsCtx())
                throw new BTDBException("Not supported.");
            _keyReader = (ReaderFun<TKey>) manipulator.RelationInfo
                .GetSimpleLoader(new RelationInfo.SimpleLoaderType(advancedEnumParamField.Handler, typeof(TKey)));
        }

        protected override TValue CreateInstance(ByteBuffer prefixKeyBytes, ByteBuffer keyBytes)
        {
            return (TValue) _manipulator.CreateInstanceFromSecondaryKey(ItemLoader, _secondaryKeyIndex,
                _prefixFieldCount, _keyBytes, keyBytes);
        }
    }
}
