using System;
using System.Collections.Generic;
using System.Diagnostics;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public class ServiceReaderCtx : IReaderCtx
    {
        readonly IServiceInternalServer _serviceServer;
        readonly IServiceInternalClient _serviceClient;
        readonly AbstractBufferedReader _reader;
        List<object> _objects;
        Stack<IMemorizedPosition> _returningStack;
        int _lastIdOfObj;

        public ServiceReaderCtx(IServiceInternalServer serviceServer, AbstractBufferedReader reader)
        {
            _serviceServer = serviceServer;
            _serviceClient = null;
            _reader = reader;
            _lastIdOfObj = 0;
        }

        public ServiceReaderCtx(IServiceInternalClient serviceClient, AbstractBufferedReader reader)
        {
            _serviceServer = null;
            _serviceClient = serviceClient;
            _reader = reader;
            _lastIdOfObj = 0;
        }

        public bool ReadObject(out object @object)
        {
            var id = (int)_reader.ReadVUInt32();
            if (id == 0)
            {
                @object = null;
                return false;
            }
            id--;
            var o = RetriveObj(id);
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
            _lastIdOfObj = id;
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

        public object ReadNativeObject()
        {
            object @object;
            if (ReadObject(out @object))
            {
                if (_serviceServer != null)
                {
                    @object = _serviceServer.LoadObjectOnServer(this);
                    ReadObjectDone();
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
            return @object;
        }

        public bool SkipObject()
        {
            var id = (int)_reader.ReadVUInt32();
            if (id == 0)
            {
                return false;
            }
            id--;
            var o = RetriveObj(id);
            if (o != null)
            {
                return false;
            }
            _objects[id] = ((ICanMemorizePosition)_reader).MemorizeCurrentPosition();
            return true;
        }

        public void SkipNativeObject()
        {
            throw new NotImplementedException();
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
    }
}