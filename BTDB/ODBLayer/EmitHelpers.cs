using System;
using System.ComponentModel;
using System.Diagnostics.SymbolStore;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using BTDB.IL;

namespace BTDB.ODBLayer
{
    internal static class EmitHelpers
    {
        internal static MethodInfo GetMethodInfo(Expression<Action> expression)
        {
            return (expression.Body as MethodCallExpression).Method;
        }

        internal static ILGenerator GetILGenerator(this MethodBuilder mb, ISymbolDocumentWriter symbolDocumentWriter, int ilsize = 64)
        {
            var ilGenerator = mb.GetILGenerator(ilsize);
            if (symbolDocumentWriter != null) ilGenerator.MarkSequencePoint(symbolDocumentWriter, 1, 1, 1, 1);
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
            ilGenerator
                .Ldarg(0)
                .Ldfld(fieldBuilder)
                .Stloc(0)
                .Ldloc(0)
                .BrfalseS(labelRet)
                .Ldloc(0)
                .Ldarg(0)
                .Ldarg(1)
                .Newobj(typeof(PropertyChangedEventArgs), typeof(string))
                .Callvirt(() => ((PropertyChangedEventHandler)null).Invoke(null, null))
                .Mark(labelRet)
                .Ret();
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
            ilGenerator
                .Ldarg(0)
                .Ldfld(fieldBuilder)
                .Stloc(0)
                .Mark(label)
                .Ldloc(0)
                .Stloc(1)
                .Ldloc(1)
                .Ldarg(1)
                .Call(add
                          ? GetMethodInfo(() => Delegate.Combine(null, null))
                          : GetMethodInfo(() => Delegate.Remove(null, null)))
                .Castclass(typePropertyChangedEventHandler)
                .Stloc(2)
                .Ldarg(0);
            ilGenerator.Emit(OpCodes.Ldflda, fieldBuilder);
            ilGenerator
                .Ldloc(2)
                .Ldloc(1);
            PropertyChangedEventHandler stub = null;
            ilGenerator
                .Call(() => Interlocked.CompareExchange(ref stub, null, null))
                .Stloc(0)
                .Ldloc(0)
                .Ldloc(1);
            ilGenerator.Emit(OpCodes.Bne_Un_S, label);
            ilGenerator.Ret();
            typeBuilder.DefineMethodOverride(methodBuilder, add ? eventPropertyChanged.GetAddMethod() : eventPropertyChanged.GetRemoveMethod());
            return methodBuilder;
        }

        internal static void GenerateJumpIfEqual(ILGenerator ilGenerator, Type type, Label jumpTo, Action<ILGenerator> loadLeft, Action<ILGenerator> loadRight)
        {
            if (type == typeof(sbyte) || type == typeof(byte) || type == typeof(short) || type == typeof(ushort)
                || type == typeof(int) || type == typeof(uint) || type == typeof(long) || type == typeof(ulong)
                || type == typeof(float) || type == typeof(double) || type == typeof(bool))
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
