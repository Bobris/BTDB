using System;
using System.Reflection.Emit;
using BTDB.IL;

namespace BTDB.ODBLayer
{
    public class SignedFieldHandler : IFieldHandler
    {
        public string Name
        {
            get { return "Signed"; }
        }

        public byte[] Configuration
        {
            get { return null; }
        }

        public bool IsCompatibleWith(Type type)
        {
            if (type == typeof(sbyte)) return true;
            if (type == typeof(short)) return true;
            if (type == typeof(int)) return true;
            if (type == typeof(long)) return true;
            return false;
        }

        public bool LoadToSameHandler(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushThis, Type implType, string destFieldName)
        {
            return false;
        }

        public Type WillLoad()
        {
            return typeof(long);
        }

        public void LoadToWillLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => ((AbstractBufferedReader)null).ReadVInt64());
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => ((AbstractBufferedReader)null).SkipVInt64());
        }

        public void CreateImpl(FieldHandlerCreateImpl ctx)
        {
            FieldBuilder fieldBuilder = FieldHandlerHelpers.GenerateSimplePropertyCreateImpl(ctx);
            var ilGenerator = ctx.Saver;
            ilGenerator
                .Ldloc(1)
                .Ldloc(0)
                .Ldfld(fieldBuilder);
            if (fieldBuilder.FieldType != typeof(long)) ilGenerator.ConvI8();
            ilGenerator.Call(() => ((AbstractBufferedWriter)null).WriteVInt64(0));
        }
    }
}