using System;
using System.Reflection;
using System.Reflection.Emit;
using BTDB.IL;

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
            ilGenerator.Call(() => ((AbstractBufferedReader)null).ReadVUInt64());
            var fieldInfo = implType.GetField("_FieldStorage_" + destFieldName);
            ilGenerator.Stfld(fieldInfo);
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
            ilGenerator.Call(() => ((AbstractBufferedReader)null).SkipVUInt64());
        }

        public void CreateImpl(FieldHandlerCreateImpl ctx)
        {
            var tb = ctx.ImplType;
            var pi = ctx.PropertyInfo;
            var fieldBuilder = tb.DefineField("_FieldStorage_" + ctx.FieldName, typeof(ulong), FieldAttributes.Public);
            var getMethodBuilder = tb.DefineMethod("get_" + pi.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, pi.PropertyType, Type.EmptyTypes);
            var ilGenerator = getMethodBuilder.GetILGenerator(ctx.SymbolDocWriter);
            ilGenerator
                .Ldarg(0)
                .Ldfld(ctx.FieldMidLevelDBTransaction)
                .Ldarg(0)
                .Ldfld(fieldBuilder)
                .Callvirt(() => ((IMidLevelDBTransactionInternal)null).Get(0));
            if (pi.PropertyType != typeof(object))
            {
                ilGenerator.Isinst(pi.PropertyType);
            }
            ilGenerator.Ret();
            tb.DefineMethodOverride(getMethodBuilder, pi.GetGetMethod());
            var setMethodBuilder = tb.DefineMethod("set_" + pi.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, typeof(void),
                                                   new[] { pi.PropertyType });
            ilGenerator = setMethodBuilder.GetILGenerator(ctx.SymbolDocWriter);
            var labelNoChange = ilGenerator.DefineLabel();
            ilGenerator.DeclareLocal(typeof(ulong));
            ilGenerator
                .Ldarg(0)
                .Ldfld(ctx.FieldMidLevelDBTransaction)
                .Ldarg(1)
                .Callvirt(() => ((IMidLevelDBTransactionInternal)null).GetOid(null))
                .Stloc(0);
            EmitHelpers.GenerateJumpIfEqual(ilGenerator, typeof(ulong), labelNoChange,
                                            g => g.Ldarg(0).Ldfld(fieldBuilder),
                                            g => g.Ldloc(0));
            ilGenerator
                .Ldarg(0)
                .Ldloc(0)
                .Stfld(fieldBuilder);
            ctx.CallObjectModified(ilGenerator);
            ilGenerator
                .Mark(labelNoChange)
                .Ret();
            tb.DefineMethodOverride(setMethodBuilder, pi.GetSetMethod());
            var propertyBuilder = tb.DefineProperty(pi.Name, PropertyAttributes.None, pi.PropertyType, Type.EmptyTypes);
            propertyBuilder.SetGetMethod(getMethodBuilder);
            propertyBuilder.SetSetMethod(setMethodBuilder);
            ilGenerator = ctx.Saver;
            ilGenerator
                .Ldloc(1)
                .Ldloc(0)
                .Ldfld(fieldBuilder)
                .Call(() => ((AbstractBufferedWriter)null).WriteVUInt64(0));
        }
    }
}