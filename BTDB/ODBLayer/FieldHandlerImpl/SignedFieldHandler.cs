using System;
using System.Reflection.Emit;

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

        public void Load(FieldHandlerLoad ctx)
        {
            if (FieldHandlerHelpers.SkipLoadIfNeeded(this, ctx)) return;
            if (!ctx.TargetTableFieldInfo.Handler.IsCompatibleWith(typeof(long)))
            {
                FieldHandlerHelpers.SkipLoad(this, ctx);
                return;
            }
            ctx.PushThis(ctx.IlGenerator);
            ctx.PushReader(ctx.IlGenerator);
            ctx.IlGenerator.Emit(OpCodes.Call, EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadVInt64()));
            var fieldInfo = ctx.ImplType.GetField("_FieldStorage_" + ctx.FieldName);
            if (fieldInfo.FieldType == typeof(long)) { }
            else if (fieldInfo.FieldType == typeof(int)) ctx.IlGenerator.Emit(OpCodes.Conv_I4);
            else if (fieldInfo.FieldType == typeof(short)) ctx.IlGenerator.Emit(OpCodes.Conv_I2);
            else if (fieldInfo.FieldType == typeof(sbyte)) ctx.IlGenerator.Emit(OpCodes.Conv_I1);
            ctx.IlGenerator.Emit(OpCodes.Stfld, fieldInfo);
        }

        public void SkipLoad(FieldHandlerSkipLoad ctx)
        {
            ctx.PushReader(ctx.IlGenerator);
            ctx.IlGenerator.Emit(OpCodes.Callvirt, EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipVInt64()));
        }
    }
}