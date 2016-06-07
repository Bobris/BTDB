using System;
using BTDB.Buffer;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class ByteArrayLastFieldHandler : ByteArrayFieldHandler
    {
        public override string Name => "Byte[]Last";

        public override void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(() => default(AbstractBufferedReader).ReadByteArrayRawTillEof());
        }

        public override void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
        }

        public override void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            pushWriterOrCtx(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(() => default(AbstractBufferedWriter).WriteByteArrayRaw(null));
        }

        protected override void SaveByteBuffer(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            pushWriterOrCtx(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(() => default(AbstractBufferedWriter).WriteBlock(ByteBuffer.NewEmpty()));
        }

        public override bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            if ((options & FieldHandlerOptions.AtEndOfStream) == 0) return false;
            return base.IsCompatibleWith(type, options);
        }
    }
}