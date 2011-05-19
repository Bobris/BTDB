using System;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class UnsignedFieldHandler : IFieldHandler
    {
        public string Name
        {
            get { return "Unsigned"; }
        }

        public byte[] Configuration
        {
            get { return null; }
        }

        public bool IsCompatibleWith(Type type)
        {
            if (type == typeof(byte)) return true;
            if (type == typeof(ushort)) return true;
            if (type == typeof(uint)) return true;
            if (type == typeof(ulong)) return true;
            return false;
        }

        public bool LoadToSameHandler(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushThis, Type implType, string destFieldName)
        {
            return false;
        }

        public Type WillLoad()
        {
            return typeof (ulong);
        }

        public void LoadToWillLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => ((AbstractBufferedReader)null).ReadVUInt64());
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => ((AbstractBufferedReader)null).SkipVUInt64());
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
            var fieldBuilder = ctx.DefaultFieldBuilder;
            var ilGenerator = ctx.Generator;
            ilGenerator
                .Ldloc(1)
                .Ldloc(0)
                .Ldfld(fieldBuilder);
            if (fieldBuilder.FieldType != typeof(ulong)) ilGenerator.ConvU8();
            ilGenerator.Call(() => ((AbstractBufferedWriter)null).WriteVUInt64(0));
        }
    }
}