using System;
using System.Reflection.Emit;

namespace BTDB.ODBLayer
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

        public void Load(FieldHandlerLoad ctx)
        {
            if (FieldHandlerHelpers.SkipLoadIfNeeded(this, ctx)) return;
            if (!ctx.TargetTableFieldInfo.Handler.IsCompatibleWith(typeof(ulong)))
            {
                FieldHandlerHelpers.SkipLoad(this, ctx);
                return;
            }
            ctx.PushThis(ctx.IlGenerator);
            ctx.PushReader(ctx.IlGenerator);
            ctx.IlGenerator.Emit(OpCodes.Call, EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadVUInt64()));
            var fieldInfo = ctx.ImplType.GetField("_FieldStorage_" + ctx.FieldName);
            if (fieldInfo.FieldType == typeof(ulong)) { }
            else if (fieldInfo.FieldType == typeof(uint)) ctx.IlGenerator.Emit(OpCodes.Conv_U4);
            else if (fieldInfo.FieldType == typeof(ushort)) ctx.IlGenerator.Emit(OpCodes.Conv_U2);
            else if (fieldInfo.FieldType == typeof(byte)) ctx.IlGenerator.Emit(OpCodes.Conv_U1);
            ctx.IlGenerator.Emit(OpCodes.Stfld, fieldInfo);
        }

        public void SkipLoad(FieldHandlerSkipLoad ctx)
        {
            ctx.PushReader(ctx.IlGenerator);
            ctx.IlGenerator.Emit(OpCodes.Callvirt, EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipVUInt64()));
        }

        public void CreateImpl(FieldHandlerCreateImpl ctx)
        {
            FieldBuilder fieldBuilder = FieldHandlerHelpers.GenerateSimplePropertyCreateImpl(ctx);
            var ilGenerator = ctx.Saver;
            ilGenerator.Emit(OpCodes.Ldloc_1);
            ilGenerator.Emit(OpCodes.Ldloc_0);
            ilGenerator.Emit(OpCodes.Ldfld, fieldBuilder);
            if (fieldBuilder.FieldType != typeof(ulong)) ilGenerator.Emit(OpCodes.Conv_U8);
            ilGenerator.Emit(OpCodes.Call, EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteVUInt64(0)));
        }
    }
}