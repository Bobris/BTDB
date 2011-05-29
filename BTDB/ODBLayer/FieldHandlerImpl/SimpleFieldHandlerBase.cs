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

        public bool LoadToSameHandler(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushThis, Type implType, string destFieldName)
        {
            return false;
        }

        public Type WillLoad()
        {
            return _loader.ReturnType;
        }

        public void LoadToWillLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(_loader);
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(_skipper);
        }

        public void SaveFromWillLoad(ILGenerator ilGenerator, Action<ILGenerator> pushWriter, Action<ILGenerator> pushValue)
        {
            pushWriter(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Call(_saver);
        }

        public void CreateStorage(FieldHandlerCreateImpl ctx)
        {
            ctx.CreateSimpleStorage();
        }

        public void CreatePropertyGetter(FieldHandlerCreateImpl ctx)
        {
            ctx.CreateSimplePropertyGetter();
        }

        public void CreatePropertySetter(FieldHandlerCreateImpl ctx)
        {
            ctx.CreateSimplePropertySetter();
        }

        public void CreateSaver(FieldHandlerCreateImpl ctx)
        {
            SaveFromWillLoad(ctx.Generator, il => il.Ldloc(1), il => il.Ldloc(0).Ldfld(ctx.DefaultFieldBuilder));
        }
    }
}