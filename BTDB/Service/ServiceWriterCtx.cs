using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.StreamLayer;

namespace BTDB.Service
{
    public class ServiceWriterCtx : IWriterCtx
    {
        Dictionary<object, uint>? _objectIdMap;
        uint _lastId;
        readonly IServiceInternalClient? _serviceClient;
        readonly IServiceInternalServer? _serviceServer;

        public ServiceWriterCtx(IServiceInternalClient serviceClient)
        {
            _serviceClient = serviceClient;
            _serviceServer = null;
        }

        public ServiceWriterCtx(IServiceInternalServer serviceServer)
        {
            _serviceClient = null;
            _serviceServer = serviceServer;
        }

        public bool WriteObject(ref SpanWriter writer, [NotNullWhen(true)] object? @object)
        {
            if (@object == null)
            {
                writer.WriteByteZero();
                return false;
            }

            if (_objectIdMap == null) _objectIdMap = new Dictionary<object, uint>();
            if (_objectIdMap.TryGetValue(@object, out var cid))
            {
                writer.WriteVUInt32(cid);
                return false;
            }

            _lastId++;
            _objectIdMap.Add(@object, _lastId);
            writer.WriteVUInt32(_lastId);
            return true;
        }

        public void WriteNativeObject(ref SpanWriter writer, object? @object)
        {
            if (WriteObject(ref writer, @object))
            {
                if (_serviceClient != null)
                {
                    _serviceClient.WriteObjectForServer(@object, ref writer, this);
                }
                else
                {
                    _serviceServer!.WriteObjectForClient(@object, ref writer, this);
                }
            }
        }

        public void WriteNativeObjectPreventInline(ref SpanWriter writer, object @object)
        {
            WriteNativeObject(ref writer, @object);
        }

        public void WriteEncryptedString(ref SpanWriter outsideWriter, EncryptedString value)
        {
            var writer = new SpanWriter();
            writer.WriteString(value);
            var cipher = _serviceClient?.GetSymmetricCipher() ?? _serviceServer?.GetSymmetricCipher();
            var plain = writer.GetSpan();
            var encSize = cipher!.CalcEncryptedSizeFor(plain);
            var enc = new byte[encSize];
            cipher.Encrypt(plain, enc);
            outsideWriter.WriteByteArray(enc);
        }

        public void WriteOrderedEncryptedString(ref SpanWriter outsideWriter, EncryptedString value)
        {
            var writer = new SpanWriter();
            writer.WriteString(value);
            var cipher = _serviceClient?.GetSymmetricCipher() ?? _serviceServer?.GetSymmetricCipher();
            var plain = writer.GetSpan();
            var encSize = cipher!.CalcOrderedEncryptedSizeFor(plain);
            var enc = new byte[encSize];
            cipher.OrderedEncrypt(plain, enc);
            outsideWriter.WriteByteArray(enc);
        }
    }
}
