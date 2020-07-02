using System;
using System.Diagnostics;
using System.Security.Cryptography;
using BTDB.Collections;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public class ServiceReaderCtx : IReaderCtx
    {
        readonly IServiceInternalServer? _serviceServer;
        readonly IServiceInternalClient? _serviceClient;
        StructList<object> _objects;
        StructList<long> _returningStack;
        int _lastIdOfObj;
        ISymmetricCipher? _cipher;

        public ServiceReaderCtx(IServiceInternalServer serviceServer)
        {
            _serviceServer = serviceServer;
            _serviceClient = null;
            _lastIdOfObj = 0;
        }

        public ServiceReaderCtx(IServiceInternalClient serviceClient)
        {
            _serviceServer = null;
            _serviceClient = serviceClient;
            _lastIdOfObj = 0;
        }

        public bool ReadObject(ref SpanReader reader, out object? @object)
        {
            var id = (int) reader.ReadVUInt32();
            if (id == 0)
            {
                @object = null;
                return false;
            }

            id--;
            var o = RetrieveObj(id);
            if (o != null)
            {
                var mp = o as IMemorizedPosition;
                if (mp == null)
                {
                    @object = o;
                    return false;
                }

                PushReturningPosition(reader.GetCurrentPosition());
                mp.Restore(ref reader);
            }
            else
            {
                PushReturningPosition(-1);
            }

            _lastIdOfObj = id;
            @object = null;
            return true;
        }

        void PushReturningPosition(long memorizedPosition)
        {
            if (_returningStack.Count == 0)
            {
                if (memorizedPosition == -1) return;
            }

            if (_returningStack.Count == 0 && memorizedPosition == -1) return;
            _returningStack.Add(memorizedPosition);
        }

        public void RegisterObject(object @object)
        {
            Debug.Assert(@object != null);
            _objects[_lastIdOfObj] = @object;
        }

        public void ReadObjectDone(ref SpanReader reader)
        {
            if (_returningStack.Count == 0) return;
            var returnPos = _returningStack.Last;
            _returningStack.RemoveAt(^1);
            if (returnPos != -1) reader.SetCurrentPosition(returnPos);
        }

        public object? ReadNativeObject(ref SpanReader reader)
        {
            if (ReadObject(ref reader, out var @object))
            {
                @object = _serviceServer != null
                    ? _serviceServer.LoadObjectOnServer(ref reader, this)
                    : _serviceClient!.LoadObjectOnClient(ref reader, this);
                ReadObjectDone(ref reader);
            }

            return @object;
        }

        public bool SkipObject(ref SpanReader reader)
        {
            var id = (int) reader.ReadVUInt32();
            if (id == 0)
            {
                return false;
            }

            id--;
            var o = RetrieveObj(id);
            if (o != null)
            {
                return false;
            }

            _objects[id] = new MemorizedPosition(reader.GetCurrentPosition());
            return true;
        }

        public void SkipNativeObject(ref SpanReader reader)
        {
            ReadNativeObject(ref reader);
        }

        object? RetrieveObj(int ido)
        {
            while (_objects.Count <= ido) _objects.Add(null);
            return _objects[ido];
        }

        public EncryptedString ReadEncryptedString(ref SpanReader reader)
        {
            if (_cipher == null)
            {
                _cipher = _serviceClient != null
                    ? _serviceClient.GetSymmetricCipher()
                    : _serviceServer!.GetSymmetricCipher();
            }

            var enc = reader.ReadByteArray();
            var size = _cipher!.CalcPlainSizeFor(enc);
            var dec = new byte[size];
            if (!_cipher.Decrypt(enc, dec))
            {
                throw new CryptographicException();
            }

            var r = new SpanReader(dec);
            return r.ReadString();
        }

        public void SkipEncryptedString(ref SpanReader reader)
        {
            reader.SkipByteArray();
        }

        public EncryptedString ReadOrderedEncryptedString(ref SpanReader reader)
        {
            if (_cipher == null)
            {
                _cipher = _serviceClient != null
                    ? _serviceClient.GetSymmetricCipher()
                    : _serviceServer!.GetSymmetricCipher();
            }

            var enc = reader.ReadByteArray();
            var size = _cipher!.CalcOrderedPlainSizeFor(enc);
            var dec = new byte[size];
            if (!_cipher.OrderedDecrypt(enc, dec))
            {
                throw new CryptographicException();
            }

            var r = new SpanReader(dec);
            return r.ReadString();
        }

        public void SkipOrderedEncryptedString(ref SpanReader reader)
        {
            reader.SkipByteArray();
        }

        public void RegisterDict(ulong dictId)
        {
            throw new InvalidOperationException();
        }

        public void RegisterOid(ulong oid)
        {
            throw new InvalidOperationException();
        }

        public void FreeContentInNativeObject(ref SpanReader reader)
        {
            throw new InvalidOperationException();
        }
    }
}
