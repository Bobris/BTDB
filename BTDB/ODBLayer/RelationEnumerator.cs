using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    class RelationEnumerator<T> : IEnumerator<T>
    {
        protected readonly IInternalObjectDBTransaction Transaction;
        protected readonly RelationInfo RelationInfo;
        readonly IRelationModificationCounter _modificationCounter;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector;
        readonly IKeyValueDBTransaction _keyValueTr;
        long _prevProtectionCounter;

        uint _pos;
        bool _seekNeeded;

        protected ByteBuffer KeyBytes;
        int _prevModificationCounter;

        public RelationEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo, ByteBuffer keyBytes, 
                                  IRelationModificationCounter modificationCounter)
        {
            RelationInfo = relationInfo;
            Transaction = tr;

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
            return (T)RelationInfo.CreateInstance(Transaction, keyBytes, valueBytes, false);
        }

        object IEnumerator.Current => Current;

        public void Reset()
        {
            throw new NotSupportedException();
        }

        public void Dispose()
        {
        }
    }

    internal class RelationPrimaryKeyEnumerator<T> : RelationEnumerator<T>
    {
        readonly int _skipBytes;

        public RelationPrimaryKeyEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo, ByteBuffer keyBytes,
                                            IRelationModificationCounter modificationCounter)
            : base(tr, relationInfo, keyBytes, modificationCounter)
        {
            _skipBytes = relationInfo.Prefix.Length;
        }

        protected override T CreateInstance(ByteBuffer keyBytes, ByteBuffer valueBytes)
        {
            var keyData = new byte[KeyBytes.Length - _skipBytes + keyBytes.Length];
            Array.Copy(KeyBytes.Buffer, KeyBytes.Offset + _skipBytes, keyData, 0, KeyBytes.Length - _skipBytes);
            Array.Copy(keyBytes.Buffer, keyBytes.Offset, keyData, KeyBytes.Length - _skipBytes, keyBytes.Length);

            return (T)RelationInfo.CreateInstance(Transaction, ByteBuffer.NewAsync(keyData), valueBytes, false);
        }

        public override ByteBuffer GetKeyBytes()
        {
            var keyBytes = base.GetKeyBytes();
            var keyData = new byte[KeyBytes.Length - ObjectDB.AllRelationsPKPrefix.Length + keyBytes.Length];
            Array.Copy(KeyBytes.Buffer, KeyBytes.Offset + ObjectDB.AllRelationsPKPrefix.Length, keyData, 0, KeyBytes.Length - ObjectDB.AllRelationsPKPrefix.Length);
            Array.Copy(keyBytes.Buffer, keyBytes.Offset, keyData, KeyBytes.Length - ObjectDB.AllRelationsPKPrefix.Length, keyBytes.Length);

            return ByteBuffer.NewAsync(keyData);
        }
    }

    internal class RelationSecondaryKeyEnumerator<T> : RelationEnumerator<T>
    {
        readonly uint _secondaryKeyIndex;
        readonly uint _fieldCountInKey;
        readonly RelationDBManipulator<T> _manipulator;

        public RelationSecondaryKeyEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo, ByteBuffer keyBytes,
            uint secondaryKeyIndex, uint fieldCountInKey, RelationDBManipulator<T> manipulator)
            : base(tr, relationInfo, keyBytes, manipulator.ModificationCounter)
        {
            _secondaryKeyIndex = secondaryKeyIndex;
            _fieldCountInKey = fieldCountInKey;
            _manipulator = manipulator;
        }

        protected override T CreateInstance(ByteBuffer keyBytes, ByteBuffer valueBytes)
        {
            return _manipulator.CreateInstanceFromSK(_secondaryKeyIndex, _fieldCountInKey, KeyBytes, keyBytes);
        }
    }

    public class RelationAdvancedEnumerator<T> : IEnumerator<T>
    {
        protected readonly uint _prefixFieldCount;
        protected readonly RelationDBManipulator<T> _manipulator;
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
            RelationDBManipulator<T> manipulator,
            ByteBuffer prefixBytes, uint prefixFieldCount,
            EnumerationOrder order,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes)
        {
            _prefixFieldCount = prefixFieldCount;
            _manipulator = manipulator;
            
            _ascending = order == EnumerationOrder.Ascending;

            _tr = manipulator.Transaction;
            _keyValueTr = _tr.KeyValueDBTransaction;
            _keyValueTrProtector = _tr.TransactionProtector;
            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;

            _keyBytes = prefixBytes;
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
            _count = (uint)Math.Max(0, endIndex - startIndex + 1);
            _startPos = (uint)(_ascending ? startIndex : endIndex);
            _pos = 0;
            _seekNeeded = true;
            _lengthOfNonDataPrefix = manipulator.RelationInfo.Prefix.Length;
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
            throw new NotSupportedException();
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

            set
            {
                throw new NotSupportedException();
            }
        }

        protected virtual T CreateInstance(ByteBuffer keyBytes)
        {
            var data = new byte[_keyBytes.Length - _lengthOfNonDataPrefix + keyBytes.Length];
            Array.Copy(_keyBytes.Buffer, _keyBytes.Offset + _lengthOfNonDataPrefix, data, 0, _keyBytes.Length - _lengthOfNonDataPrefix);
            Array.Copy(keyBytes.Buffer, 0, data, _keyBytes.Length - _lengthOfNonDataPrefix, keyBytes.Length);

            return (T)_manipulator.RelationInfo.CreateInstance(_tr, ByteBuffer.NewAsync(data), _keyValueTr.GetValue(), false);
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
    }

    public class RelationAdvancedSecondaryKeyEnumerator<T> : RelationAdvancedEnumerator<T>
    {
        readonly uint _secondaryKeyIndex;

        public RelationAdvancedSecondaryKeyEnumerator(
            RelationDBManipulator<T> manipulator,
            ByteBuffer prefixBytes, uint prefixFieldCount,
            EnumerationOrder order,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes,
            uint secondaryKeyIndex)
            : base(manipulator, prefixBytes, prefixFieldCount, order,
                  startKeyProposition, startKeyBytes,
                  endKeyProposition, endKeyBytes)
        {
            _secondaryKeyIndex = secondaryKeyIndex;
        }

        protected override T CreateInstance(ByteBuffer keyBytes)
        {
            return _manipulator.CreateInstanceFromSK(_secondaryKeyIndex, _prefixFieldCount, _keyBytes, keyBytes);
        }
    }

    public class RelationAdvancedOrderedEnumerator<TKey, TValue> : IOrderedDictionaryEnumerator<TKey, TValue>
    {
        protected readonly uint _prefixFieldCount;
        protected readonly RelationDBManipulator<TValue> _manipulator;
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
        protected Func<AbstractBufferedReader, IReaderCtx, TKey> _keyReader;
        readonly int _lengthOfNonDataPrefix;

        public RelationAdvancedOrderedEnumerator(RelationDBManipulator<TValue> manipulator,
            ByteBuffer prefixBytes, uint prefixFieldCount,
            EnumerationOrder order,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes, bool initKeyReader = true)
        {
            _prefixFieldCount = prefixFieldCount;
            _manipulator = manipulator;
            _tr = manipulator.Transaction;
            _ascending = order == EnumerationOrder.Ascending;

            _keyValueTr = _tr.KeyValueDBTransaction;
            _keyValueTrProtector = _tr.TransactionProtector;
            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;

            _keyBytes = prefixBytes;
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
            _count = (uint)Math.Max(0, endIndex - startIndex + 1);
            _startPos = (uint)(_ascending ? startIndex : endIndex);
            _pos = 0;
            _seekState = SeekState.Undefined;

            if (initKeyReader)
            {
                var primaryKeyFields = manipulator.RelationInfo.ClientRelationVersionInfo.GetPrimaryKeyFields();
                var advancedEnumParamField = primaryKeyFields.ToList()[(int)_prefixFieldCount];
                if (advancedEnumParamField.Handler.NeedsCtx())
                    throw new BTDBException("Not supported.");
                _keyReader = (Func<AbstractBufferedReader, IReaderCtx, TKey>)manipulator.RelationInfo
                    .GetSimpleLoader(new RelationInfo.SimpleLoaderType(advancedEnumParamField.Handler, typeof(TKey)));

                _lengthOfNonDataPrefix = manipulator.RelationInfo.Prefix.Length;
            }
            
        }

        public uint Count => _count;

        protected virtual TValue CreateInstance(ByteBuffer prefixKeyBytes, ByteBuffer keyBytes)
        {
            var data = new byte[_keyBytes.Length - _lengthOfNonDataPrefix + keyBytes.Length];
            Array.Copy(_keyBytes.Buffer, _keyBytes.Offset + _lengthOfNonDataPrefix, data, 0, _keyBytes.Length - _lengthOfNonDataPrefix);
            Array.Copy(keyBytes.Buffer, 0, data, _keyBytes.Length - _lengthOfNonDataPrefix, keyBytes.Length);

            return (TValue)_manipulator.RelationInfo.CreateInstance(_tr, ByteBuffer.NewAsync(data), _keyValueTr.GetValue(), false);
        }

        public TValue CurrentValue
        {
            get
            {
                if (_pos >= _count) throw new IndexOutOfRangeException();
                if (_seekState == SeekState.Undefined) throw new BTDBException("Invalid access to uninitialized CurrentValue.");
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
            set
            {
                throw new NotSupportedException();
            }
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
            get { return _pos; }

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
            //read key
            var keyData = _keyValueTr.GetKeyAsByteArray();
            var reader = new ByteArrayReader(keyData);
            key = _keyReader(reader, null);
            return true;
        }
    }

    public class RelationAdvancedOrderedSecondaryKeyEnumerator<TKey, TValue> :
        RelationAdvancedOrderedEnumerator<TKey, TValue>
    {
        readonly uint _secondaryKeyIndex;

        public RelationAdvancedOrderedSecondaryKeyEnumerator(RelationDBManipulator<TValue> manipulator,
            ByteBuffer prefixBytes, uint prefixFieldCount, EnumerationOrder order,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes,
            uint secondaryKeyIndex)
            : base(manipulator, prefixBytes, prefixFieldCount, order,
                   startKeyProposition, startKeyBytes,
                   endKeyProposition, endKeyBytes, false)
        {
            _secondaryKeyIndex = secondaryKeyIndex;
            var secKeyFields = manipulator.RelationInfo.ClientRelationVersionInfo.GetSecondaryKeyFields(secondaryKeyIndex);
            var advancedEnumParamField = secKeyFields.ToList()[(int)_prefixFieldCount];
            if (advancedEnumParamField.Handler.NeedsCtx())
                throw new BTDBException("Not supported.");
            _keyReader = (Func<AbstractBufferedReader, IReaderCtx, TKey>)manipulator.RelationInfo
                .GetSimpleLoader(new RelationInfo.SimpleLoaderType(advancedEnumParamField.Handler, typeof(TKey)));
        }

        protected override TValue CreateInstance(ByteBuffer prefixKeyBytes, ByteBuffer keyBytes)
        {
            return _manipulator.CreateInstanceFromSK(_secondaryKeyIndex, _prefixFieldCount, _keyBytes, keyBytes);
        }
    }
}
