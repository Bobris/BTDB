using System;
using System.Collections;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    class RelationEnumerator<T> : IEnumerator<T>
    {
        readonly IInternalObjectDBTransaction _tr;
        protected readonly RelationInfo RelationInfo;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector;
        readonly IKeyValueDBTransaction _keyValueTr;
        long _prevProtectionCounter;

        uint _pos;
        bool _seekNeeded;

        readonly ByteBuffer _keyBytes;

        public RelationEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo, ByteBuffer keyBytes)
        {
            RelationInfo = relationInfo;
            _tr = tr;

            _keyValueTr = _tr.KeyValueDBTransaction;
            _keyValueTrProtector = _tr.TransactionProtector;
            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;

            _keyBytes = keyBytes;
            _keyValueTr.SetKeyPrefix(_keyBytes);
            _pos = 0;
            _seekNeeded = true;
        }

        public bool MoveNext()
        {
            if (!_seekNeeded)
                _pos++;
            return Seek();
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
                _keyValueTrProtector.Start();
                if (_keyValueTrProtector.WasInterupted(_prevProtectionCounter))
                {
                    _keyValueTr.SetKeyPrefix(_keyBytes);
                    Seek();
                }
                else if (_seekNeeded)
                {
                    Seek();
                    _seekNeeded = false;
                }
                _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                var keyBytes = _keyValueTr.GetKey();
                var valueBytes = _keyValueTr.GetValue();
                return CreateInstance(keyBytes, valueBytes);
            }
        }

        protected virtual T CreateInstance(ByteBuffer keyBytes, ByteBuffer valueBytes)
        {
            return (T)RelationInfo.CreateInstance(_tr, keyBytes, valueBytes, false);
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

    internal class RelationSecondaryKeyEnumerator<T> : RelationEnumerator<T>
    {
        readonly uint _secondaryKeyIndex;
        readonly RelationDBManipulator<T> _manipulator;

        public RelationSecondaryKeyEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo, ByteBuffer keyBytes,
            uint secondaryKeyIndex, RelationDBManipulator<T> manipulator)
            : base(tr, relationInfo, keyBytes)
        {
            _secondaryKeyIndex = secondaryKeyIndex;
            _manipulator = manipulator;
        }

        protected override T CreateInstance(ByteBuffer keyBytes, ByteBuffer valueBytes)
        {
            return _manipulator.CreateInstanceFromSK(_secondaryKeyIndex, keyBytes, valueBytes);
        }
    }

    internal class RelationAdvancedSecondaryKeyEnumerator<T> : IEnumerator<T>
    {
        readonly IInternalObjectDBTransaction _tr;
        readonly RelationInfo _relationInfo;
        readonly uint _secondaryKeyIndex;
        readonly string _secondaryKeyName;
        readonly RelationDBManipulator<T> _manipulator;

        readonly KeyValueDBTransactionProtector _keyValueTrProtector;
        readonly IKeyValueDBTransaction _keyValueTr;
        long _prevProtectionCounter;
        readonly uint _startPos = 0;
        readonly uint _count = 0;
        uint _pos;
        bool _seekNeeded;
        readonly bool _ascending;

        readonly ByteBuffer _keyBytes;

        public RelationAdvancedSecondaryKeyEnumerator(IInternalObjectDBTransaction tr, RelationInfo relationInfo,
            AdvancedEnumeratorParam<T> param,
            uint secondaryKeyIndex, RelationDBManipulator<T> manipulator)
        {
            _relationInfo = relationInfo;
            _secondaryKeyIndex = secondaryKeyIndex;
            _secondaryKeyName = _relationInfo.ClientRelationVersionInfo.SecondaryKeys[_secondaryKeyIndex].Name;
            _manipulator = manipulator;
            _tr = tr;

            _keyValueTr = _tr.KeyValueDBTransaction;
            _keyValueTrProtector = _tr.TransactionProtector;
            _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;

            _keyBytes = BuildKeyBytes();
            _keyValueTr.SetKeyPrefix(ObjectDB.AllRelationsSKPrefix);

            long startIndex;
            long endIndex;
            if (param.EndProposition == KeyProposition.Ignored)
            {
                endIndex = _keyValueTr.GetKeyValueCount() - 1;
            }
            else
            {
                var keyBytes = _manipulator.WriteSecodaryKeyKey(_secondaryKeyIndex,
                    _secondaryKeyName, param.End);
                switch (_keyValueTr.Find(keyBytes))
                {
                    case FindResult.Exact:
                        endIndex = _keyValueTr.GetKeyIndex();
                        if (param.EndProposition == KeyProposition.Excluded)
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
            if (param.StartProposition == KeyProposition.Ignored)
            {
                startIndex = 0;
            }
            else
            {
                var keyBytes = _manipulator.WriteSecodaryKeyKey(_secondaryKeyIndex,
                    _secondaryKeyName, param.Start);
                switch (_keyValueTr.Find(keyBytes))
                {
                    case FindResult.Exact:
                        startIndex = _keyValueTr.GetKeyIndex();
                        if (param.StartProposition == KeyProposition.Excluded)
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
            _keyValueTr.SetKeyPrefix(_keyBytes);
            _count = (uint)Math.Max(0, endIndex - startIndex + 1);
            _startPos = (uint)(_ascending ? startIndex : endIndex);
            _pos = 0;
            _seekNeeded = true;
        }

        ByteBuffer BuildKeyBytes()
        {
            var keyWriter = new ByteBufferWriter();
            keyWriter.WriteByteArrayRaw(ObjectDB.AllRelationsSKPrefix);
            keyWriter.WriteVUInt32(_relationInfo.Id);
            keyWriter.WriteVUInt32(_secondaryKeyIndex);
            return keyWriter.Data.ToAsyncSafe();
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
                _keyValueTrProtector.Start();
                if (_keyValueTrProtector.WasInterupted(_prevProtectionCounter))
                {
                    _keyValueTr.SetKeyPrefix(_keyBytes);
                    Seek();
                }
                else if (_seekNeeded)
                {
                    Seek();
                    _seekNeeded = false;
                }
                _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                var keyBytes = _keyValueTr.GetKey();
                var valueBytes = _keyValueTr.GetValue();
                return _manipulator.CreateInstanceFromSK(_secondaryKeyIndex, keyBytes, valueBytes);
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
            _seekNeeded = false;
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }
    }

}