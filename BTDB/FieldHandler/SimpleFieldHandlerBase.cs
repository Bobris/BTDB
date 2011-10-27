using System;
using System.Reflection;
using System.Reflection.Emit;
using BTDB.IL;

namespace BTDB.FieldHandler
{
    public abstract class SimpleFieldHandlerJustOrderableBase : SimpleFieldHandlerBase
    {
        protected SimpleFieldHandlerJustOrderableBase(MethodInfo loader, MethodInfo skipper, MethodInfo saver):base(loader,skipper,saver)
        {
        }

        public override bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            if (!options.HasFlag(FieldHandlerOptions.Orderable)) return false;
            return base.IsCompatibleWith(type, options);
        }
    }

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

        public void Load(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(_loader);
        }

        public void Skip(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            if (_skipper == null) return;
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(_skipper);
        }

        public void Save(ILGenerator ilGenerator, Action<ILGenerator> pushWriterOrCtx, Action<ILGenerator> pushValue)
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