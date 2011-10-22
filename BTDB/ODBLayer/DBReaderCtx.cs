using System.Collections.Generic;
using System.Diagnostics;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class DBReaderCtx : IReaderCtx, IInstanceRegistry
    {
        readonly IInternalObjectDBTransaction _transaction;
        readonly AbstractBufferedReader _reader;
        List<object> _objects;
        Stack<IMemorizedPosition> _returningStack;
        int _lastIdOfObj;

        public DBReaderCtx(IInternalObjectDBTransaction transaction, AbstractBufferedReader reader)
        {
            _transaction = transaction;
            _reader = reader;
            _lastIdOfObj = -1;
        }

        public bool ReadObject(out object @object)
        {
            var id = _reader.ReadVInt64();
            if (id == 0)
            {
                @object = null;
                return false;
            }
            if (id <= int.MinValue || id > 0)
            {
                @object = _transaction.Get((ulong)id);
                return false;
            }
            var ido = (int)(-id) - 1;
            var o = RetriveObj(ido);
            if (o != null)
            {
                var mp = o as IMemorizedPosition;
                if (mp == null)
                {
                    @object = o;
                    return false;
                }
                PushReturningPosition(((ICanMemorizePosition)_reader).MemorizeCurrentPosition());
                mp.Restore();
            }
            else
            {
                PushReturningPosition(null);
            }
            _lastIdOfObj = ido;
            @object = null;
            return true;
        }

        void PushReturningPosition(IMemorizedPosition memorizedPosition)
        {
            if (_returningStack == null)
            {
                if (memorizedPosition == null) return;
                _returningStack = new Stack<IMemorizedPosition>();
            }
            if (_returningStack.Count == 0 && memorizedPosition == null) return;
            _returningStack.Push(memorizedPosition);
        }

        public void RegisterObject(object @object)
        {
            Debug.Assert(@object != null);
            _objects[_lastIdOfObj] = @object;
        }

        public void ReadObjectDone()
        {
            if (_returningStack == null) return;
            if (_returningStack.Count == 0) return;
            var returnPos = _returningStack.Pop();
            if (returnPos != null) returnPos.Restore();
        }

        public bool SkipObject()
        {
            var id = _reader.ReadVInt64();
            if (id == 0)
            {
                return false;
            }
            if (id <= int.MinValue || id > 0)
            {
                return false;
            }
            var ido = (int)(-id) - 1;
            var o = RetriveObj(ido);
            if (o != null)
            {
                return false;
            }
            _objects[ido] = ((ICanMemorizePosition)_reader).MemorizeCurrentPosition();
            return true;
        }

        object RetriveObj(int ido)
        {
            if (_objects == null) _objects = new List<object>();
            while (_objects.Count <= ido) _objects.Add(null);
            return _objects[ido];
        }

        public AbstractBufferedReader Reader()
        {
            return _reader;
        }

        public int RegisterInstance(object content)
        {
            return ((IInstanceRegistry) _transaction.Owner).RegisterInstance(content);
        }

        public object FindInstance(int id)
        {
            return ((IInstanceRegistry) _transaction.Owner).FindInstance(id);
        }
    }
}