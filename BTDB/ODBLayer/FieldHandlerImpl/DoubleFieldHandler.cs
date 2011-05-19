using System;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class DoubleFieldHandler : IFieldHandler
    {
        public string Name
        {
            get { return "Double"; }
        }

        public byte[] Configuration
        {
            get { return null; }
        }

        public bool IsCompatibleWith(Type type)
        {
            return type == typeof(double);
        }

        public bool LoadToSameHandler(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushThis, Type implType, string destFieldName)
        {
            return false;
        }

        public Type WillLoad()
        {
            return typeof(double);
        }

        public void LoadToWillLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => ((AbstractBufferedReader)null).ReadDouble());
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => ((AbstractBufferedReader)null).SkipDouble());
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
            ctx.Generator
                .Ldloc(1)
                .Ldloc(0)
                .Ldfld(ctx.DefaultFieldBuilder)
                .Call(() => ((AbstractBufferedWriter)null).WriteDouble(0));
        }
    }
}