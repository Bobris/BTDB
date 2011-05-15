using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal static class ILGeneratorExtensions
    {
        internal static ILGenerator LdcI4(this ILGenerator il, int value)
        {
            switch (value)
            {
                case 0:
                    il.Emit(OpCodes.Ldc_I4_0);
                    break;
                case 1:
                    il.Emit(OpCodes.Ldc_I4_1);
                    break;
                case 2:
                    il.Emit(OpCodes.Ldc_I4_2);
                    break;
                case 3:
                    il.Emit(OpCodes.Ldc_I4_3);
                    break;
                case 4:
                    il.Emit(OpCodes.Ldc_I4_4);
                    break;
                case 5:
                    il.Emit(OpCodes.Ldc_I4_5);
                    break;
                case 6:
                    il.Emit(OpCodes.Ldc_I4_6);
                    break;
                case 7:
                    il.Emit(OpCodes.Ldc_I4_7);
                    break;
                case 8:
                    il.Emit(OpCodes.Ldc_I4_8);
                    break;
                case -1:
                    il.Emit(OpCodes.Ldc_I4_M1);
                    break;
                default:
                    if (value >= -128 && value <= 127)
                        il.Emit(OpCodes.Ldc_I4_S, (sbyte)value);
                    else
                        il.Emit(OpCodes.Ldc_I4, value);
                    break;
            }
            return il;
        }

        internal static ILGenerator Ldarg(this ILGenerator il, ushort parameterIndex)
        {
            switch (parameterIndex)
            {
                case 0:
                    il.Emit(OpCodes.Ldarg_0);
                    break;
                case 1:
                    il.Emit(OpCodes.Ldarg_1);
                    break;
                case 2:
                    il.Emit(OpCodes.Ldarg_2);
                    break;
                case 3:
                    il.Emit(OpCodes.Ldarg_3);
                    break;
                default:
                    if (parameterIndex <= 255)
                        il.Emit(OpCodes.Ldarg_S, (byte)parameterIndex);
                    else
                        il.Emit(OpCodes.Ldarg, parameterIndex);
                    break;
            }
            return il;
        }

        internal static ILGenerator Ldfld(this ILGenerator il, FieldInfo fieldInfo)
        {
            il.Emit(OpCodes.Ldfld, fieldInfo);
            return il;
        }

        internal static ILGenerator Ldflda(this ILGenerator il, FieldInfo fieldInfo)
        {
            il.Emit(OpCodes.Ldflda, fieldInfo);
            return il;
        }

        internal static ILGenerator Stfld(this ILGenerator il, FieldInfo fieldInfo)
        {
            il.Emit(OpCodes.Stfld, fieldInfo);
            return il;
        }

        internal static ILGenerator Stloc(this ILGenerator il, ushort localVariableIndex)
        {
            switch (localVariableIndex)
            {
                case 0:
                    il.Emit(OpCodes.Stloc_0);
                    break;
                case 1:
                    il.Emit(OpCodes.Stloc_1);
                    break;
                case 2:
                    il.Emit(OpCodes.Stloc_2);
                    break;
                case 3:
                    il.Emit(OpCodes.Stloc_3);
                    break;
                case 65535:
                    throw new ArgumentOutOfRangeException("localVariableIndex");
                default:
                    if (localVariableIndex <= 255)
                        il.Emit(OpCodes.Stloc_S, (byte)localVariableIndex);
                    else
                        il.Emit(OpCodes.Stloc, localVariableIndex);
                    break;
            }
            return il;
        }

        internal static ILGenerator Stloc(this ILGenerator il, LocalBuilder localBuilder)
        {
            il.Emit(OpCodes.Stloc, localBuilder);
            return il;
        }

        internal static ILGenerator Ldloc(this ILGenerator il, ushort localVariableIndex)
        {
            switch (localVariableIndex)
            {
                case 0:
                    il.Emit(OpCodes.Ldloc_0);
                    break;
                case 1:
                    il.Emit(OpCodes.Ldloc_1);
                    break;
                case 2:
                    il.Emit(OpCodes.Ldloc_2);
                    break;
                case 3:
                    il.Emit(OpCodes.Ldloc_3);
                    break;
                case 65535:
                    throw new ArgumentOutOfRangeException("localVariableIndex");
                default:
                    if (localVariableIndex <= 255)
                        il.Emit(OpCodes.Ldloc_S, (byte)localVariableIndex);
                    else
                        il.Emit(OpCodes.Ldloc, localVariableIndex);
                    break;
            }
            return il;
        }

        internal static ILGenerator Ldloc(this ILGenerator il, LocalBuilder localBuilder)
        {
            il.Emit(OpCodes.Ldloc, localBuilder);
            return il;
        }

        internal static ILGenerator Ldloca(this ILGenerator il, LocalBuilder localBuilder)
        {
            il.Emit(OpCodes.Ldloca, localBuilder);
            return il;
        }

        internal static ILGenerator Brfalse(this ILGenerator il, Label targetLabel)
        {
            il.Emit(OpCodes.Brfalse, targetLabel);
            return il;
        }

        internal static ILGenerator BrfalseS(this ILGenerator il, Label targetLabel)
        {
            il.Emit(OpCodes.Brfalse_S, targetLabel);
            return il;
        }

        internal static ILGenerator Brtrue(this ILGenerator il, Label targetLabel)
        {
            il.Emit(OpCodes.Brtrue, targetLabel);
            return il;
        }

        internal static ILGenerator BrtrueS(this ILGenerator il, Label targetLabel)
        {
            il.Emit(OpCodes.Brtrue_S, targetLabel);
            return il;
        }

        internal static ILGenerator BrS(this ILGenerator il, Label targetLabel)
        {
            il.Emit(OpCodes.Br_S, targetLabel);
            return il;
        }

        internal static ILGenerator BneUnS(this ILGenerator il, Label targetLabel)
        {
            il.Emit(OpCodes.Bne_Un_S, targetLabel);
            return il;
        }

        internal static ILGenerator BeqS(this ILGenerator il, Label targetLabel)
        {
            il.Emit(OpCodes.Beq_S, targetLabel);
            return il;
        }

        internal static ILGenerator Newobj(this ILGenerator il, Expression<Action> expression)
        {
            var constructorInfo = (expression.Body as NewExpression).Constructor;
            il.Emit(OpCodes.Newobj, constructorInfo);
            return il;
        }

        internal static ILGenerator Newobj(this ILGenerator il, ConstructorInfo constructorInfo)
        {
            il.Emit(OpCodes.Newobj, constructorInfo);
            return il;
        }

        internal static ILGenerator Callvirt(this ILGenerator il, Expression<Action> expression)
        {
            var methodInfo = (expression.Body as MethodCallExpression).Method;
            if (methodInfo.IsStatic) throw new ArgumentException("Method in Callvirt cannot be static");
            il.Emit(OpCodes.Callvirt, methodInfo);
            return il;
        }

        internal static ILGenerator Callvirt(this ILGenerator il, MethodInfo methodInfo)
        {
            if (methodInfo.IsStatic) throw new ArgumentException("Method in Callvirt cannot be static");
            il.Emit(OpCodes.Callvirt, methodInfo);
            return il;
        }

        internal static ILGenerator Call(this ILGenerator il, Expression<Action> expression)
        {
            var newExpression = expression.Body as NewExpression;
            if (newExpression!=null)
            {
                il.Emit(OpCodes.Call, newExpression.Constructor);
            }
            else
            {
                var methodInfo = (expression.Body as MethodCallExpression).Method;
                il.Emit(OpCodes.Call, methodInfo);
            }
            return il;
        }

        internal static ILGenerator Call(this ILGenerator il, MethodInfo methodInfo)
        {
            il.Emit(OpCodes.Call, methodInfo);
            return il;
        }

        internal static ILGenerator Call(this ILGenerator il, ConstructorInfo constructorInfo)
        {
            il.Emit(OpCodes.Call, constructorInfo);
            return il;
        }

        internal static ILGenerator Mark(this ILGenerator il, Label label)
        {
            il.MarkLabel(label);
            return il;
        }

        internal static ILGenerator Ldstr(this ILGenerator il, string str)
        {
            il.Emit(OpCodes.Ldstr, str);
            return il;
        }

        internal static ILGenerator Throw(this ILGenerator il)
        {
            il.Emit(OpCodes.Throw);
            return il;
        }

        internal static ILGenerator Ret(this ILGenerator il)
        {
            il.Emit(OpCodes.Ret);
            return il;
        }

        internal static ILGenerator Castclass(this ILGenerator il, Type toType)
        {
            il.Emit(OpCodes.Castclass, toType);
            return il;
        }

        internal static ILGenerator Isinst(this ILGenerator il, Type asType)
        {
            il.Emit(OpCodes.Isinst, asType);
            return il;
        }

        internal static ILGenerator ConvU1(this ILGenerator il)
        {
            il.Emit(OpCodes.Conv_U1);
            return il;
        }

        internal static ILGenerator ConvU2(this ILGenerator il)
        {
            il.Emit(OpCodes.Conv_U2);
            return il;
        }

        internal static ILGenerator ConvU4(this ILGenerator il)
        {
            il.Emit(OpCodes.Conv_U4);
            return il;
        }

        internal static ILGenerator ConvU8(this ILGenerator il)
        {
            il.Emit(OpCodes.Conv_U8);
            return il;
        }

        internal static ILGenerator ConvI1(this ILGenerator il)
        {
            il.Emit(OpCodes.Conv_I1);
            return il;
        }

        internal static ILGenerator ConvI2(this ILGenerator il)
        {
            il.Emit(OpCodes.Conv_I2);
            return il;
        }

        internal static ILGenerator ConvI4(this ILGenerator il)
        {
            il.Emit(OpCodes.Conv_I4);
            return il;
        }

        internal static ILGenerator ConvI8(this ILGenerator il)
        {
            il.Emit(OpCodes.Conv_I8);
            return il;
        }

    }
}