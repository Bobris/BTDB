using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal static class ILGeneratorExtensions
    {
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
                    il.Emit(parameterIndex <= 255 ? OpCodes.Ldarg_S : OpCodes.Ldarg, parameterIndex);
                    break;
            }
            return il;
        }

        internal static ILGenerator Ldfld(this ILGenerator il, FieldInfo fieldInfo)
        {
            il.Emit(OpCodes.Ldfld, fieldInfo);
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
                    il.Emit(localVariableIndex <= 255 ? OpCodes.Stloc_S : OpCodes.Stloc, localVariableIndex);
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
                    il.Emit(localVariableIndex <= 255 ? OpCodes.Ldloc_S : OpCodes.Ldloc, localVariableIndex);
                    break;
            }
            return il;
        }

        internal static ILGenerator Ldloc(this ILGenerator il, LocalBuilder localBuilder)
        {
            il.Emit(OpCodes.Ldloc, localBuilder);
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

        internal static ILGenerator Newobj(this ILGenerator il, Type objectType, params Type[] parameterTypes)
        {
            il.Emit(OpCodes.Newobj, objectType.GetConstructor(parameterTypes));
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
            var methodInfo = (expression.Body as MethodCallExpression).Method;
            il.Emit(OpCodes.Call, methodInfo);
            return il;
        }

        internal static ILGenerator Call(this ILGenerator il, MethodInfo methodInfo)
        {
            il.Emit(OpCodes.Call, methodInfo);
            return il;
        }

        internal static ILGenerator Mark(this ILGenerator il, Label label)
        {
            il.MarkLabel(label);
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

    }
}