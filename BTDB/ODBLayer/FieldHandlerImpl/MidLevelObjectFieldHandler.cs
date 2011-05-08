using System;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.ODBLayer
{
    public class MidLevelObjectFieldHandler : IFieldHandler
    {
        public string Name
        {
            get { return "DBObject"; }
        }

        public byte[] Configuration
        {
            get { return null; }
        }

        public bool IsCompatibleWith(Type type)
        {
            return type == typeof(IMidLevelObject) || (type.IsInterface && !type.IsGenericTypeDefinition);
        }

        public bool LoadToSameHandler(ILGenerator ilGenerator, Action<ILGenerator> pushReader, Action<ILGenerator> pushThis, Type implType, string destFieldName)
        {
            pushThis(ilGenerator);
            pushReader(ilGenerator);
            ilGenerator.Emit(OpCodes.Call, EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).ReadVUInt64()));
            var fieldInfo = implType.GetField("_FieldStorage_" + destFieldName);
            ilGenerator.Emit(OpCodes.Stfld, fieldInfo);
            return true;
        }

        public Type WillLoad()
        {
            return null;
        }

        public void LoadToWillLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            throw new InvalidOperationException();
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReader)
        {
            pushReader(ilGenerator);
            ilGenerator.Emit(OpCodes.Call, EmitHelpers.GetMethodInfo(() => ((AbstractBufferedReader)null).SkipVUInt64()));
        }

        public void CreateImpl(FieldHandlerCreateImpl ctx)
        {
            var tb = ctx.ImplType;
            var pi = ctx.PropertyInfo;
            var fieldBuilder = tb.DefineField("_FieldStorage_" + ctx.FieldName, typeof(ulong), FieldAttributes.Public);
            var getMethodBuilder = tb.DefineMethod("get_" + pi.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, pi.PropertyType, Type.EmptyTypes);
            var ilGenerator = getMethodBuilder.GetILGenerator(ctx.SymbolDocWriter);
            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Ldfld, ctx.FieldMidLevelDBTransaction);
            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Ldfld, fieldBuilder);
            ilGenerator.Emit(OpCodes.Callvirt, EmitHelpers.GetMethodInfo(() => ((IMidLevelDBTransactionInternal)null).Get(0)));
            if (pi.PropertyType!=typeof(object))
            {
                ilGenerator.Emit(OpCodes.Isinst, pi.PropertyType);
            }
            ilGenerator.Emit(OpCodes.Ret);
            tb.DefineMethodOverride(getMethodBuilder, pi.GetGetMethod());
            var setMethodBuilder = tb.DefineMethod("set_" + pi.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, typeof(void),
                                                   new[] { pi.PropertyType });
            ilGenerator = setMethodBuilder.GetILGenerator(ctx.SymbolDocWriter);
            var labelNoChange = ilGenerator.DefineLabel();
            ilGenerator.DeclareLocal(typeof(ulong));
            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Ldfld, ctx.FieldMidLevelDBTransaction);
            ilGenerator.Emit(OpCodes.Ldarg_1);
            ilGenerator.Emit(OpCodes.Callvirt, EmitHelpers.GetMethodInfo(() => ((IMidLevelDBTransactionInternal)null).GetOid(null)));
            ilGenerator.Emit(OpCodes.Stloc_0);
            EmitHelpers.GenerateJumpIfEqual(ilGenerator, typeof(ulong), labelNoChange,
                                            g =>
                                                {
                                                    g.Emit(OpCodes.Ldarg_0);
                                                    g.Emit(OpCodes.Ldfld, fieldBuilder);
                                                },
                                            g => g.Emit(OpCodes.Ldloc_0));
            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Ldloc_0);
            ilGenerator.Emit(OpCodes.Stfld, fieldBuilder);
            ctx.CallObjectModified(ilGenerator);
            ilGenerator.MarkLabel(labelNoChange);
            ilGenerator.Emit(OpCodes.Ret);
            tb.DefineMethodOverride(setMethodBuilder, pi.GetSetMethod());
            var propertyBuilder = tb.DefineProperty(pi.Name, PropertyAttributes.None, pi.PropertyType, Type.EmptyTypes);
            propertyBuilder.SetGetMethod(getMethodBuilder);
            propertyBuilder.SetSetMethod(setMethodBuilder);
            ilGenerator = ctx.Saver;
            ilGenerator.Emit(OpCodes.Ldloc_1);
            ilGenerator.Emit(OpCodes.Ldloc_0);
            ilGenerator.Emit(OpCodes.Ldfld, fieldBuilder);
            ilGenerator.Emit(OpCodes.Call, EmitHelpers.GetMethodInfo(() => ((AbstractBufferedWriter)null).WriteVUInt64(0)));
        }
    }
}