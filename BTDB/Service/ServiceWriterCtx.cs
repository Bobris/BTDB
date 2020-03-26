using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public class ServiceWriterCtx : IWriterCtx
    {
        readonly AbstractBufferedWriter _writer;
        Dictionary<object, uint>? _objectIdMap;
        uint _lastId;
        readonly IServiceInternalClient? _serviceClient;
        readonly IServiceInternalServer? _serviceServer;

        public ServiceWriterCtx(IServiceInternalClient serviceClient, AbstractBufferedWriter writer)
        {
            _serviceClient = serviceClient;
            _serviceServer = null;
            _writer = writer;
        }

        public ServiceWriterCtx(IServiceInternalServer serviceServer, AbstractBufferedWriter writer)
        {
            _serviceClient = null;
            _serviceServer = serviceServer;
            _writer = writer;
        }

        public bool WriteObject([NotNullWhen(true)] object? @object)
        {
            if (@object == null)
            {
                _writer.WriteByteZero();
                return false;
            }
            if (_objectIdMap == null) _objectIdMap = new Dictionary<object, uint>();
            uint cid;
            if (_objectIdMap.TryGetValue(@object, out cid))
            {
                _writer.WriteVUInt32(cid);
                return false;
            }
            _lastId++;
            _objectIdMap.Add(@object, _lastId);
            _writer.WriteVUInt32(_lastId);
            return true;
        }

        public void WriteNativeObject(object? @object)
        {
            if (WriteObject(@object))
            {
                if (_serviceClient != null)
                {
                    _serviceClient.WriteObjectForServer(@object, this);
                }
                else
                {
                    _serviceServer!.WriteObjectForClient(@object, this);
                }
            }
        }

        public void WriteNativeObjectPreventInline(object @object)
        {
            WriteNativeObject(@object);
        }

        public AbstractBufferedWriter Writer()
        {
            return _writer;
        }

        public void WriteEncryptedString(EncryptedString value)
        {
            var writer = new ByteBufferWriter();
            writer.WriteString(value);
            var cipher = _serviceClient?.GetSymmetricCipher() ?? _serviceServer?.GetSymmetricCipher();
            var plain = writer.Data.AsSyncReadOnlySpan();
            var encSize = cipher!.CalcEncryptedSizeFor(plain);
            var enc = new byte[encSize];
            cipher.Encrypt(plain, enc);
            _writer.WriteByteArray(enc);
        }

        public void WriteOrderedEncryptedString(EncryptedString value)
        {
            var writer = new ByteBufferWriter();
            writer.WriteString(value);
            var cipher = _serviceClient?.GetSymmetricCipher() ?? _serviceServer?.GetSymmetricCipher();
            var plain = writer.Data.AsSyncReadOnlySpan();
            var encSize = cipher!.CalcOrderedEncryptedSizeFor(plain);
            var enc = new byte[encSize];
            cipher.OrderedEncrypt(plain, enc);
            _writer.WriteByteArray(enc);
        }
    }
}
