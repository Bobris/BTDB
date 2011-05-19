using System;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class DBObjectFieldHandler : IFieldHandler
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
            return type == typeof(IDBObject) || (type.IsInterface && !type.IsGenericTypeDefinition);
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

        public void CreateStorage(FieldHandlerCreateImpl ctx)
        {
            ctx.CreateSimpleStorage(typeof(ulong));
        }

        public void CreatePropertyGetter(FieldHandlerCreateImpl ctx)
        {
            var ilGenerator = ctx.Generator;
            ilGenerator
                .Ldarg(0)
                .Ldfld(ctx.FieldMidLevelDBTransaction)
                .Ldarg(0)
                .Ldfld(ctx.DefaultFieldBuilder)
                .Callvirt(() => ((IObjectDBTransactionInternal)null).Get(0));
            if (ctx.PropertyInfo.PropertyType != typeof(object))
            {
                ilGenerator.Isinst(ctx.PropertyInfo.PropertyType);
            }
        }

        public void CreatePropertySetter(FieldHandlerCreateImpl ctx)
        {
            var ilGenerator = ctx.Generator;
            var fieldBuilder = ctx.DefaultFieldBuilder;
            var labelNoChange = ilGenerator.DefineLabel();
            ilGenerator.DeclareLocal(typeof(ulong));
            ilGenerator
                .Ldarg(0)
                .Ldfld(ctx.FieldMidLevelDBTransaction)
                .Ldarg(1)
                .Callvirt(() => ((IObjectDBTransactionInternal)null).GetOid(null))
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
                .Mark(labelNoChange);
        }

        public void CreateSaver(FieldHandlerCreateImpl ctx)
        {
            ctx.Generator
                .Ldloc(1)
                .Ldloc(0)
                .Ldfld(ctx.DefaultFieldBuilder)
                .Call(() => ((AbstractBufferedWriter)null).WriteVUInt64(0));
        }
    }
}