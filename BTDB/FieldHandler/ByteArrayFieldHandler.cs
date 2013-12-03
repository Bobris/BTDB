using System;
using BTDB.Buffer;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class ByteArrayFieldHandler : IFieldHandler
    {
        public virtual string Name
        {
            get { return "Byte[]"; }
        }

        public byte[] Configuration
        {
            get { return null; }
        }

        public virtual bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return typeof(byte[]) == type || typeof(ByteBuffer) == type;
        }

        public Type HandledType()
        {
            return typeof(byte[]);
        }

        public bool NeedsCtx()
        {
            return false;
        }

        public virtual void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(() => default(AbstractBufferedReader).ReadByteArray());
        }

        public virtual void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(() => default(AbstractBufferedReader).SkipByteArray());
        }

        public virtual void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            pushWriterOrCtx(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(() => default(AbstractBufferedWriter).WriteByteArray(null));
        }

        protected virtual void SaveByteBuffer(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            pushWriterOrCtx(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(() => default(AbstractBufferedWriter).WriteByteArray(ByteBuffer.NewEmpty()));
        }

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler typeHandler)
        {
            if (typeof (ByteBuffer) == type)
            {
                return new ByteBufferHandler(this);
            }
            return this;
        }

        class ByteBufferHandler : IFieldHandler
        {
            readonly ByteArrayFieldHandler _fieldHandler;

            public ByteBufferHandler(ByteArrayFieldHandler fieldHandler)
            {
                _fieldHandler = fieldHandler;
            }

            public string Name { get { return _fieldHandler.Name; } }
            public byte[] Configuration { get { return _fieldHandler.Configuration; } }

            public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
            {
                return typeof(ByteBuffer) == type;
            }

            public Type HandledType()
            {
                return typeof(ByteBuffer);
            }

            public bool NeedsCtx()
            {
                return false;
            }

            public void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
            {
                _fieldHandler.Load(ilGenerator, pushReaderOrCtx);
                ilGenerator.Call(() => ByteBuffer.NewAsync(null));
            }

            public void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
            {
                _fieldHandler.Skip(ilGenerator, pushReaderOrCtx);
            }

            public void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
            {
                _fieldHandler.SaveByteBuffer(ilGenerator, pushWriterOrCtx, pushValue);
            }

            public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler typeHandler)
            {
                throw new InvalidOperationException();
            }

            public IFieldHandler SpecializeSaveForType(Type type)
            {
                throw new InvalidOperationException();
            }
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            if (typeof(ByteBuffer) == type)
            {
                return new ByteBufferHandler(this);
            }
            return this;
        }
    }
}