using System;
using System.Reflection;
using System.Reflection.Emit;
using BTDB.IL;

namespace BTDB.ODBLayer
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

        public virtual bool IsCompatibleWith(Type type)
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

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(_skipper);
        }

        public void Save(ILGenerator ilGenerator, Action<ILGenerator> pushWriterOrCtx, Action<ILGenerator> pushValue)
        {
            pushWriterOrCtx(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(_saver);
        }

        public void InformAboutDestinationHandler(IFieldHandler dstHandler)
        {
        }
    }
}