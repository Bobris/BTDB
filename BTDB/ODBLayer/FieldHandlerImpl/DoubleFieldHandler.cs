using System;
using System.Reflection.Emit;
using BTDB.IL;

namespace BTDB.ODBLayer
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

        public void CreateImpl(FieldHandlerCreateImpl ctx)
        {
            FieldBuilder fieldBuilder = FieldHandlerHelpers.GenerateSimplePropertyCreateImpl(ctx);
            var ilGenerator = ctx.Saver;
            ilGenerator
                .Ldloc(1)
                .Ldloc(0)
                .Ldfld(fieldBuilder)
                .Call(() => ((AbstractBufferedWriter)null).WriteDouble(0));
        }
    }
}