using System;
using System.Reflection;
using System.Reflection.Emit;
using BTDB.IL;

namespace BTDB.FieldHandler
{
    public abstract class SimpleFieldOrderableHandlerBase: SimpleFieldHandlerBase, IFieldHandleOrderable
    {
        readonly MethodInfo _loaderOrdered;
        readonly MethodInfo _skipperOrdered;
        readonly MethodInfo _saverOrdered;

        protected SimpleFieldOrderableHandlerBase(MethodInfo loader, MethodInfo skipper, MethodInfo saver, MethodInfo loaderOrdered, MethodInfo skipperOrdered, MethodInfo saverOrdered)
            : base(loader, skipper, saver)
        {
            _loaderOrdered = loaderOrdered;
            _skipperOrdered = skipperOrdered;
            _saverOrdered = saverOrdered;
        }

        protected SimpleFieldOrderableHandlerBase(MethodInfo loader, MethodInfo skipper, MethodInfo saver)
            : base(loader, skipper, saver)
        {
            _loaderOrdered = loader;
            _skipperOrdered = skipper;
            _saverOrdered = saver;
        }

        public void LoadOrdered(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(_loaderOrdered);
        }

        public void SkipOrdered(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Call(_skipperOrdered);
        }

        public void SaveOrdered(ILGenerator ilGenerator, Action<ILGenerator> pushWriterOrCtx, Action<ILGenerator> pushValue)
        {
            pushWriterOrCtx(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(_saverOrdered);
        }
    }
}