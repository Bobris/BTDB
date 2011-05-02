using System;
using System.ComponentModel;
using System.Diagnostics.SymbolStore;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;

namespace BTDB.ODBLayer
{
    internal static class EmitHelpers
    {
        internal static MethodInfo GetMethodInfo(Expression<Action> expression)
        {
            return (expression.Body as MethodCallExpression).Method;
        }

        internal static ILGenerator GetILGenerator(this MethodBuilder mb,ISymbolDocumentWriter symbolDocumentWriter, int ilsize=64)
        {
            var ilGenerator = mb.GetILGenerator(ilsize);
            if (symbolDocumentWriter!=null) ilGenerator.MarkSequencePoint(symbolDocumentWriter, 1, 1, 1, 1);
            return ilGenerator;
        }

        internal static MethodBuilder GenerateINotifyPropertyChangedImpl(TypeBuilder typeBuilder, ISymbolDocumentWriter symbolDocumentWriter)
        {
            var fieldBuilder = typeBuilder.DefineField("_propertyChanged", typeof(PropertyChangedEventHandler), FieldAttributes.Private);
            var eventBuilder = typeBuilder.DefineEvent("PropertyChanged", EventAttributes.None, typeof(PropertyChangedEventHandler));
            eventBuilder.SetAddOnMethod(GenerateAddRemoveEvent(typeBuilder, fieldBuilder, true));
            eventBuilder.SetRemoveOnMethod(GenerateAddRemoveEvent(typeBuilder, fieldBuilder, false));
            var methodBuilder = typeBuilder.DefineMethod("RaisePropertyChanged", MethodAttributes.Family, null, new[] { typeof(string) });
            var ilGenerator = methodBuilder.GetILGenerator();
            ilGenerator.MarkSequencePoint(symbolDocumentWriter, 1, 1, 1, 1);
            ilGenerator.DeclareLocal(typeof(PropertyChangedEventHandler));
            var labelRet = ilGenerator.DefineLabel();
            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Ldfld, fieldBuilder);
            ilGenerator.Emit(OpCodes.Stloc_0);
            ilGenerator.Emit(OpCodes.Ldloc_0);
            ilGenerator.Emit(OpCodes.Brfalse_S, labelRet);
            ilGenerator.Emit(OpCodes.Ldloc_0);
            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Ldarg_1);
            ilGenerator.Emit(OpCodes.Newobj, typeof(PropertyChangedEventArgs).GetConstructor(new[] { typeof(string) }));
            ilGenerator.Emit(OpCodes.Callvirt, typeof(PropertyChangedEventHandler).GetMethod("Invoke", new[] { typeof(object), typeof(PropertyChangedEventArgs) }));
            ilGenerator.MarkLabel(labelRet);
            ilGenerator.Emit(OpCodes.Ret);
            return methodBuilder;
        }

        static MethodBuilder GenerateAddRemoveEvent(TypeBuilder typeBuilder, FieldBuilder fieldBuilder, bool add)
        {
            Type typePropertyChangedEventHandler = typeof(PropertyChangedEventHandler);
            EventInfo eventPropertyChanged = typeof(INotifyPropertyChanged).GetEvent("PropertyChanged");
            var methodBuilder = typeBuilder.DefineMethod((add ? "add" : "remove") + "_PropertyChanged",
                                                         MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName |
                                                         MethodAttributes.HideBySig | MethodAttributes.NewSlot | MethodAttributes.Final,
                                                         typeof(void), new[] { typePropertyChangedEventHandler });
            var ilGenerator = methodBuilder.GetILGenerator();
            ilGenerator.DeclareLocal(typePropertyChangedEventHandler);
            ilGenerator.DeclareLocal(typePropertyChangedEventHandler);
            ilGenerator.DeclareLocal(typePropertyChangedEventHandler);
            var label = ilGenerator.DefineLabel();
            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Ldfld, fieldBuilder);
            ilGenerator.Emit(OpCodes.Stloc_0);
            ilGenerator.MarkLabel(label);
            ilGenerator.Emit(OpCodes.Ldloc_0);
            ilGenerator.Emit(OpCodes.Stloc_1);
            ilGenerator.Emit(OpCodes.Ldloc_1);
            ilGenerator.Emit(OpCodes.Ldarg_1);
            ilGenerator.Emit(OpCodes.Call,
                             add
                                 ? GetMethodInfo(() => Delegate.Combine(null, null))
                                 : GetMethodInfo(() => Delegate.Remove(null, null)));
            ilGenerator.Emit(OpCodes.Castclass, typePropertyChangedEventHandler);
            ilGenerator.Emit(OpCodes.Stloc_2);
            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Ldflda, fieldBuilder);
            ilGenerator.Emit(OpCodes.Ldloc_2);
            ilGenerator.Emit(OpCodes.Ldloc_1);
            PropertyChangedEventHandler stub = null;
            ilGenerator.Emit(OpCodes.Call, GetMethodInfo(() => Interlocked.CompareExchange(ref stub, null, null)));
            ilGenerator.Emit(OpCodes.Stloc_0);
            ilGenerator.Emit(OpCodes.Ldloc_0);
            ilGenerator.Emit(OpCodes.Ldloc_1);
            ilGenerator.Emit(OpCodes.Bne_Un_S, label);
            ilGenerator.Emit(OpCodes.Ret);
            typeBuilder.DefineMethodOverride(methodBuilder, add ? eventPropertyChanged.GetAddMethod() : eventPropertyChanged.GetRemoveMethod());
            return methodBuilder;
        }

        internal static void GenerateJumpIfEqual(ILGenerator ilGenerator, Type type, Label jumpTo, Action<ILGenerator> loadLeft, Action<ILGenerator> loadRight)
        {
            if (type == typeof(sbyte) || type == typeof(byte) || type == typeof(short) || type == typeof(ushort)
                || type == typeof(int) || type == typeof(uint) || type == typeof(long) || type == typeof(ulong)
                || type == typeof(float) || type == typeof(double))
            {
                loadLeft(ilGenerator);
                loadRight(ilGenerator);
                ilGenerator.Emit(OpCodes.Beq_S, jumpTo);
                return;
            }
            if (type.IsGenericType)
            {
                var genType = type.GetGenericTypeDefinition();
                if (genType == typeof(Nullable<>))
                {
                    var localLeft = ilGenerator.DeclareLocal(type);
                    var localRight = ilGenerator.DeclareLocal(type);
                    var hasValueMethod = type.GetMethod("get_HasValue");
                    var getValueMethod = type.GetMethod("GetValueOrDefault", Type.EmptyTypes);
                    loadLeft(ilGenerator);
                    ilGenerator.Emit(OpCodes.Stloc, localLeft);
                    loadRight(ilGenerator);
                    ilGenerator.Emit(OpCodes.Stloc, localRight);
                    var labelLeftHasValue = ilGenerator.DefineLabel();
                    var labelDifferent = ilGenerator.DefineLabel();
                    ilGenerator.Emit(OpCodes.Ldloca_S, localLeft);
                    ilGenerator.Emit(OpCodes.Call, hasValueMethod);
                    ilGenerator.Emit(OpCodes.Brtrue_S, labelLeftHasValue);
                    ilGenerator.Emit(OpCodes.Ldloca_S, localRight);
                    ilGenerator.Emit(OpCodes.Call, hasValueMethod);
                    ilGenerator.Emit(OpCodes.Brtrue_S, labelDifferent);
                    ilGenerator.Emit(OpCodes.Br_S, jumpTo);
                    ilGenerator.MarkLabel(labelLeftHasValue);
                    ilGenerator.Emit(OpCodes.Ldloca_S, localRight);
                    ilGenerator.Emit(OpCodes.Call, hasValueMethod);
                    ilGenerator.Emit(OpCodes.Brfalse_S, labelDifferent);
                    GenerateJumpIfEqual(ilGenerator, type.GetGenericArguments()[0], jumpTo, g =>
                                                                                                {
                                                                                                    ilGenerator.Emit(OpCodes.Ldloca_S, localLeft);
                                                                                                    g.Emit(OpCodes.Call, getValueMethod);
                                                                                                }, g =>
                                                                                                       {
                                                                                                           ilGenerator.Emit(OpCodes.Ldloca_S, localRight);
                                                                                                           g.Emit(OpCodes.Call, getValueMethod);
                                                                                                       });
                    ilGenerator.MarkLabel(labelDifferent);
                    return;
                }
            }
            var equalsMethod = type.GetMethod("Equals", new[] { type, type });
            if (equalsMethod != null)
            {
                loadLeft(ilGenerator);
                loadRight(ilGenerator);
                ilGenerator.Emit(OpCodes.Call, equalsMethod);
                ilGenerator.Emit(OpCodes.Brtrue_S, jumpTo);
                return;
            }
            throw new NotImplementedException(String.Format("Don't know how to compare type {0}", type));
        }
    }
}
