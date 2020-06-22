using System;
using BTDB.Buffer;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class ByteArrayLastFieldHandler : ByteArrayFieldHandler
    {
        public override string Name => "Byte[]Last";

        public override void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => default(SpanReader).ReadByteArrayRawTillEof());
        }

        public override void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
        {
        }

        public override void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx, Action<IILGen> pushValue)
        {
            pushWriter(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(() => default(SpanWriter).WriteByteArrayRaw(null));
        }

        protected override void SaveByteBuffer(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx, Action<IILGen> pushValue)
        {
            pushWriter(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(() => default(SpanWriter).WriteBlock(ByteBuffer.NewEmpty()));
        }

        public override bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            if ((options & FieldHandlerOptions.AtEndOfStream) == 0) return false;
            return base.IsCompatibleWith(type, options);
        }
    }
}
