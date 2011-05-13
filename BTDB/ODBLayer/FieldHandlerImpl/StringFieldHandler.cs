using System;
using System.Reflection.Emit;
using BTDB.IL;

namespace BTDB.ODBLayer
{
    public class StringFieldHandler : IFieldHandler
    {
        public string Name
        {
            get { return "String"; }
        }

        public byte[] Configuration
        {
            get { return null; }
        }

        public bool IsCompatibleWith(Type type)
        {
            return type == typeof(string);
        }

        public bool LoadToSameHandler(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushThis, Type implType, string destFieldName)
        {
            return false;
        }

        public Type WillLoad()
        {
            return typeof(string);
        }

        public void LoadToWillLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => ((AbstractBufferedReader)null).ReadString());
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            ilGenerator.Call(() => ((AbstractBufferedReader)null).SkipString());
        }

        public void CreateImpl(FieldHandlerCreateImpl ctx)
        {
            FieldBuilder fieldBuilder = FieldHandlerHelpers.GenerateSimplePropertyCreateImpl(ctx);
            var ilGenerator = ctx.Saver;
            ilGenerator
                .Ldloc(1)
                .Ldloc(0)
                .Ldfld(fieldBuilder)
                .Call(() => ((AbstractBufferedWriter)null).WriteString(null));
        }
    }
}