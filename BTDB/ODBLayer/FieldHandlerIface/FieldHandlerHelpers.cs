using System;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.ODBLayer
{
    public static class FieldHandlerHelpers
    {
        internal static FieldBuilder GenerateSimplePropertyCreateImpl(FieldHandlerCreateImpl ctx)
        {
            var tb = ctx.ImplType;
            var pi = ctx.PropertyInfo;
            var fieldBuilder = tb.DefineField("_FieldStorage_" + ctx.FieldName, pi.PropertyType, FieldAttributes.Public);
            var getMethodBuilder = tb.DefineMethod("get_" + pi.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, pi.PropertyType, Type.EmptyTypes);
            var ilGenerator = getMethodBuilder.GetILGenerator(ctx.SymbolDocWriter, 16);
            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Ldfld, fieldBuilder);
            ilGenerator.Emit(OpCodes.Ret);
            tb.DefineMethodOverride(getMethodBuilder, pi.GetGetMethod());
            var setMethodBuilder = tb.DefineMethod("set_" + pi.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, typeof(void),
                                                   new[] { pi.PropertyType });
            ilGenerator = setMethodBuilder.GetILGenerator(ctx.SymbolDocWriter);
            var labelNoChange = ilGenerator.DefineLabel();
            EmitHelpers.GenerateJumpIfEqual(ilGenerator, pi.PropertyType, labelNoChange,
                                            g =>
                                                {
                                                    g.Emit(OpCodes.Ldarg_0);
                                                    g.Emit(OpCodes.Ldfld, fieldBuilder);
                                                },
                                            g => g.Emit(OpCodes.Ldarg_1));
            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Ldarg_1);
            ilGenerator.Emit(OpCodes.Stfld, fieldBuilder);
            ctx.CallObjectModified(ilGenerator);
            ilGenerator.MarkLabel(labelNoChange);
            ilGenerator.Emit(OpCodes.Ret);
            tb.DefineMethodOverride(setMethodBuilder, pi.GetSetMethod());
            var propertyBuilder = tb.DefineProperty(pi.Name, PropertyAttributes.None, pi.PropertyType, Type.EmptyTypes);
            propertyBuilder.SetGetMethod(getMethodBuilder);
            propertyBuilder.SetSetMethod(setMethodBuilder);
            return fieldBuilder;
        }
    }
}