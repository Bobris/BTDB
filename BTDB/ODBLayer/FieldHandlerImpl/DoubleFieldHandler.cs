using System;
using System.Reflection.Emit;

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

        public void Load(FieldHandlerLoad ctx)
        {
            if (FieldHandlerHelpers.SkipLoadIfNeeded(this, ctx)) return;
            if (!ctx.TargetTableFieldInfo.Handler.IsCompatibleWith(typeof(double)))
            {
                FieldHandlerHelpers.SkipLoad(this, ctx);
                return;
            }
            ctx.PushThis(ctx.IlGenerator);
            ctx.PushReader(ctx.IlGenerator);
            ctx.IlGenerator.Emit(OpCodes.Call, EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadDouble()));
            var fieldInfo = ctx.ImplType.GetField("_FieldStorage_" + ctx.FieldName);
            ctx.IlGenerator.Emit(OpCodes.Stfld, fieldInfo);
        }

        public void SkipLoad(FieldHandlerSkipLoad ctx)
        {
            ctx.PushReader(ctx.IlGenerator);
            ctx.IlGenerator.Emit(OpCodes.Call, EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipDouble()));
        }

        public void CreateImpl(FieldHandlerCreateImpl ctx)
        {
            FieldBuilder fieldBuilder = FieldHandlerHelpers.GenerateSimplePropertyCreateImpl(ctx);
            var ilGenerator = ctx.Saver;
            ilGenerator.Emit(OpCodes.Ldloc_1);
            ilGenerator.Emit(OpCodes.Ldloc_0);
            ilGenerator.Emit(OpCodes.Ldfld, fieldBuilder);
            ilGenerator.Emit(OpCodes.Call, EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteDouble(0)));
        }
    }
}