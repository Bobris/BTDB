using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    public static class ILGenExtensions
    {
        public static IILGen Do(this IILGen il, Action<IILGen> action)
        {
            action(il);
            return il;
        }

        public static IILGen LdcI4(this IILGen il, int value)
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

        public static IILGen Ldarg(this IILGen il, ushort parameterIndex)
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

        public static IILGen Starg(this IILGen il, ushort parameterIndex)
        {
            if (parameterIndex <= 255)
                il.Emit(OpCodes.Starg_S, (byte)parameterIndex);
            else
                il.Emit(OpCodes.Starg, parameterIndex);
            return il;
        }

        public static IILGen Ldfld(this IILGen il, FieldInfo fieldInfo)
        {
            il.Emit(OpCodes.Ldfld, fieldInfo);
            return il;
        }

        public static IILGen Ldflda(this IILGen il, FieldInfo fieldInfo)
        {
            il.Emit(OpCodes.Ldflda, fieldInfo);
            return il;
        }

        public static IILGen Ldsfld(this IILGen il, FieldInfo fieldInfo)
        {
            il.Emit(OpCodes.Ldsfld, fieldInfo);
            return il;
        }

        public static IILGen Stfld(this IILGen il, FieldInfo fieldInfo)
        {
            il.Emit(OpCodes.Stfld, fieldInfo);
            return il;
        }

        public static IILGen Stsfld(this IILGen il, FieldInfo fieldInfo)
        {
            il.Emit(OpCodes.Stsfld, fieldInfo);
            return il;
        }

        public static IILGen Stloc(this IILGen il, ushort localVariableIndex)
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
                    throw new ArgumentOutOfRangeException(nameof(localVariableIndex));
                default:
                    if (localVariableIndex <= 255)
                        il.Emit(OpCodes.Stloc_S, (byte)localVariableIndex);
                    else
                        il.Emit(OpCodes.Stloc, localVariableIndex);
                    break;
            }
            return il;
        }

        public static IILGen Stloc(this IILGen il, IILLocal localBuilder)
        {
            il.Emit(OpCodes.Stloc, localBuilder);
            return il;
        }

        public static IILGen Ldloc(this IILGen il, ushort localVariableIndex)
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
                    throw new ArgumentOutOfRangeException(nameof(localVariableIndex));
                default:
                    if (localVariableIndex <= 255)
                        il.Emit(OpCodes.Ldloc_S, (byte)localVariableIndex);
                    else
                        il.Emit(OpCodes.Ldloc, localVariableIndex);
                    break;
            }
            return il;
        }

        public static IILGen Ldloc(this IILGen il, IILLocal localBuilder)
        {
            il.Emit(OpCodes.Ldloc, localBuilder);
            return il;
        }

        public static IILGen Ldloca(this IILGen il, IILLocal localBuilder)
        {
            il.Emit(OpCodes.Ldloca, localBuilder);
            return il;
        }

        public static IILGen Constrained(this IILGen il, Type type)
        {
            il.Emit(OpCodes.Constrained, type);
            return il;
        }

        public static IILGen Blt(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Blt, targetLabel);
            return il;
        }

        public static IILGen Bgt(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Bgt, targetLabel);
            return il;
        }

        public static IILGen Brfalse(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Brfalse, targetLabel);
            return il;
        }

        public static IILGen BrfalseS(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Brfalse_S, targetLabel);
            return il;
        }

        public static IILGen Brtrue(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Brtrue, targetLabel);
            return il;
        }

        public static IILGen BrtrueS(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Brtrue_S, targetLabel);
            return il;
        }

        public static IILGen Br(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Br, targetLabel);
            return il;
        }

        public static IILGen BrS(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Br_S, targetLabel);
            return il;
        }

        public static IILGen BneUnS(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Bne_Un_S, targetLabel);
            return il;
        }

        public static IILGen BeqS(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Beq_S, targetLabel);
            return il;
        }

        public static IILGen Beq(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Beq, targetLabel);
            return il;
        }

        public static IILGen BgeUnS(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Bge_Un_S, targetLabel);
            return il;
        }

        public static IILGen BgeUn(this IILGen il, IILLabel targetLabel)
        {
            il.Emit(OpCodes.Bge_Un, targetLabel);
            return il;
        }

        public static IILGen Newobj(this IILGen il, ConstructorInfo constructorInfo)
        {
            il.Emit(OpCodes.Newobj, constructorInfo);
            return il;
        }

        public static IILGen Callvirt(this IILGen il, MethodInfo methodInfo)
        {
            if (methodInfo.IsStatic) throw new ArgumentException("Method in Callvirt cannot be static");
            il.Emit(OpCodes.Callvirt, methodInfo);
            return il;
        }

        public static IILGen Call(this IILGen il, MethodInfo methodInfo)
        {
            il.Emit(OpCodes.Call, methodInfo);
            return il;
        }

        public static IILGen Call(this IILGen il, ConstructorInfo constructorInfo)
        {
            il.Emit(OpCodes.Call, constructorInfo);
            return il;
        }

        public static IILGen Ldftn(this IILGen il, MethodInfo methodInfo)
        {
            il.Emit(OpCodes.Ldftn, methodInfo);
            return il;
        }

        public static IILGen Ldnull(this IILGen il)
        {
            il.Emit(OpCodes.Ldnull);
            return il;
        }

        public static IILGen Throw(this IILGen il)
        {
            il.Emit(OpCodes.Throw);
            return il;
        }

        public static IILGen Ret(this IILGen il)
        {
            il.Emit(OpCodes.Ret);
            return il;
        }

        public static IILGen Pop(this IILGen il)
        {
            il.Emit(OpCodes.Pop);
            return il;
        }

        public static IILGen Castclass(this IILGen il, Type toType)
        {
            il.Emit(OpCodes.Castclass, toType);
            return il;
        }

        public static IILGen Isinst(this IILGen il, Type asType)
        {
            il.Emit(OpCodes.Isinst, asType);
            return il;
        }

        public static IILGen ConvU1(this IILGen il)
        {
            il.Emit(OpCodes.Conv_U1);
            return il;
        }

        public static IILGen ConvU2(this IILGen il)
        {
            il.Emit(OpCodes.Conv_U2);
            return il;
        }

        public static IILGen ConvU4(this IILGen il)
        {
            il.Emit(OpCodes.Conv_U4);
            return il;
        }

        public static IILGen ConvU8(this IILGen il)
        {
            il.Emit(OpCodes.Conv_U8);
            return il;
        }

        public static IILGen ConvI1(this IILGen il)
        {
            il.Emit(OpCodes.Conv_I1);
            return il;
        }

        public static IILGen ConvI2(this IILGen il)
        {
            il.Emit(OpCodes.Conv_I2);
            return il;
        }

        public static IILGen ConvI4(this IILGen il)
        {
            il.Emit(OpCodes.Conv_I4);
            return il;
        }

        public static IILGen ConvI8(this IILGen il)
        {
            il.Emit(OpCodes.Conv_I8);
            return il;
        }

        public static IILGen ConvR4(this IILGen il)
        {
            il.Emit(OpCodes.Conv_R4);
            return il;
        }

        public static IILGen ConvR8(this IILGen il)
        {
            il.Emit(OpCodes.Conv_R8);
            return il;
        }

        public static IILGen Tail(this IILGen il)
        {
            il.Emit(OpCodes.Tailcall);
            return il;
        }

        public static IILGen LdelemRef(this IILGen il)
        {
            il.Emit(OpCodes.Ldelem_Ref);
            return il;
        }

        public static IILGen StelemRef(this IILGen il)
        {
            il.Emit(OpCodes.Stelem_Ref);
            return il;
        }

        public static IILGen Add(this IILGen il)
        {
            il.Emit(OpCodes.Add);
            return il;
        }

        public static IILGen Sub(this IILGen il)
        {
            il.Emit(OpCodes.Sub);
            return il;
        }

        public static IILGen Mul(this IILGen il)
        {
            il.Emit(OpCodes.Mul);
            return il;
        }

        public static IILGen Div(this IILGen il)
        {
            il.Emit(OpCodes.Div);
            return il;
        }

        public static IILGen Dup(this IILGen il)
        {
            il.Emit(OpCodes.Dup);
            return il;
        }

        public static IILGen Ldtoken(this IILGen il, Type type)
        {
            il.Emit(OpCodes.Ldtoken, type);
            return il;
        }
        
        public static IILGen Callvirt(this IILGen il, Expression<Action> expression)
        {
            var methodInfo = (expression.Body as MethodCallExpression).Method;
            return il.Callvirt(methodInfo);
        }

        public static IILGen Callvirt<T>(this IILGen il, Expression<Func<T>> expression)
        {
            var newExpression = expression.Body as MemberExpression;
            if (newExpression != null)
            {
                return il.Callvirt(((PropertyInfo)newExpression.Member).GetGetMethod(true));
            }
            var methodInfo = (expression.Body as MethodCallExpression).Method;
            return il.Callvirt(methodInfo);
        }

        public static IILGen Call(this IILGen il, Expression<Action> expression)
        {
            var newExpression = expression.Body as NewExpression;
            if (newExpression != null)
            {
                return il.Call(newExpression.Constructor);
            }
            var methodInfo = (expression.Body as MethodCallExpression).Method;
            return il.Call(methodInfo);
        }

        public static IILGen Call<T>(this IILGen il, Expression<Func<T>> expression)
        {
            var memberExpression = expression.Body as MemberExpression;
            if (memberExpression != null)
            {
                return il.Call(((PropertyInfo)memberExpression.Member).GetGetMethod(true));
            }
            var newExpression = expression.Body as NewExpression;
            if (newExpression != null)
            {
                return il.Call(newExpression.Constructor);
            }
            var methodInfo = (expression.Body as MethodCallExpression).Method;
            return il.Call(methodInfo);
        }

        public static IILGen Newobj(this IILGen il, Expression<Action> expression)
        {
            var constructorInfo = (expression.Body as NewExpression).Constructor;
            return il.Newobj(constructorInfo);
        }

        public static IILGen Ldfld<T>(this IILGen il, Expression<Func<T>> expression)
        {
            return il.Ldfld((FieldInfo)(expression.Body as MemberExpression).Member);
        }

        public static IILGen Newarr(this IILGen il, Type arrayMemberType)
        {
            il.Emit(OpCodes.Newarr, arrayMemberType);
            return il;
        }

        public static IILGen Box(this IILGen il, Type boxedType)
        {
            il.Emit(OpCodes.Box, boxedType);
            return il;
        }

        public static IILGen Unbox(this IILGen il, Type valueType)
        {
            if (!valueType.IsValueType) throw new ArgumentException("Unboxed could be only valuetype");
            il.Emit(OpCodes.Unbox, valueType);
            return il;
        }

        public static IILGen UnboxAny(this IILGen il, Type anyType)
        {
            il.Emit(OpCodes.Unbox_Any, anyType);
            return il;
        }

        public static IILGen Break(this IILGen il)
        {
            il.Emit(OpCodes.Break);
            return il;
        }
    }
}