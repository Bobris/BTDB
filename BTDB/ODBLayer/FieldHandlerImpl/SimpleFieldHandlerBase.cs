using System;
using System.Reflection;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer.FieldHandlerImpl
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

        public void Load(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushCtx)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(_loader);
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushCtx)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(_skipper);
        }

        public void Save(ILGenerator ilGenerator, Action<ILGenerator> pushWriter, Action<ILGenerator> pushCtx, Action<ILGenerator> pushValue)
        {
            pushWriter(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(_saver);
        }

        public void InformAboutDestinationHandler(IFieldHandler dstHandler)
        {
        }
    }
}