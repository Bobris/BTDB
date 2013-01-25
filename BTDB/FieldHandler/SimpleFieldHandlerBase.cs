using System;
using System.Reflection;
using BTDB.IL;

namespace BTDB.FieldHandler
{
    public abstract class SimpleFieldHandlerBase : IFieldHandler
    {
        readonly MethodInfo _loader;
        readonly MethodInfo _skipper;
        readonly MethodInfo _saver;

        protected SimpleFieldHandlerBase(MethodInfo loader, MethodInfo skipper, MethodInfo saver)
        {
            _loader = loader;
            _skipper = skipper;
            _saver = saver;
        }

        public abstract string Name { get; }

        public byte[] Configuration
        {
            get { return null; }
        }

        public virtual bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return _loader.ReturnType == type;
        }

        public Type HandledType()
        {
            return _loader.ReturnType;
        }

        public bool NeedsCtx()
        {
            return false;
        }

        public void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(_loader);
        }

        public void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            if (_skipper == null) return;
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(_skipper);
        }

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            pushWriterOrCtx(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(_saver);
        }

        public IFieldHandler SpecializeLoadForType(Type type)
        {
            return this;
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            return this;
        }
    }
}