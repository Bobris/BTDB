using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.SymbolStore;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.ODBLayer
{
    internal class TableInfo
    {
        readonly uint _id;
        readonly string _name;
        uint _lastVersion;
        uint _clientTypeVersion;
        Type _clientType;
        Type _implType;
        readonly ConcurrentDictionary<uint, TableVersionInfo> _tableVersions = new ConcurrentDictionary<uint, TableVersionInfo>();
        Func<IMidLevelDBTransactionInternal, object> _inserter;
        Action<IMidLevelDBTransactionInternal> _saver;
        ConcurrentDictionary<uint, Func<IMidLevelDBTransactionInternal, object>> _loaders = new ConcurrentDictionary<uint, Func<IMidLevelDBTransactionInternal, object>>();

        internal TableInfo(uint id, string name)
        {
            _id = id;
            _name = name;
        }

        internal uint Id
        {
            get { return _id; }
        }

        internal string Name
        {
            get { return _name; }
        }

        internal Type ClientType
        {
            get { return _clientType; }
            set
            {
                _clientType = value;
                _clientTypeVersion = 0;
            }
        }

        void EnsureImplType()
        {
            if (_implType != null) return;
            System.Threading.Interlocked.CompareExchange(ref _implType, CreateImplType(Name, ClientType), null);
        }

        static Type CreateImplType(string name, Type clientType)
        {
            AssemblyBuilder ab = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name + "Asm"), AssemblyBuilderAccess.RunAndSave);
            ModuleBuilder mb = ab.DefineDynamicModule(name + "asm.dll", true);
            var symbolDocumentWriter = mb.DefineDocument("just_dynamic_" + name, Guid.Empty, Guid.Empty, Guid.Empty);
            TypeBuilder tb = mb.DefineType(name + "Impl", TypeAttributes.Public, typeof(object), new[] { clientType, typeof(INotifyPropertyChanged) });
            var raiseINPCMethod = GenerateINotifyPropertyChangedImpl(tb, symbolDocumentWriter);
            var properties = clientType.GetProperties();
            var oidFieldBuilder = tb.DefineField("Oid", typeof(ulong), FieldAttributes.InitOnly | FieldAttributes.Public);
            var trFieldBuilder = tb.DefineField("MidLevelDBTransaction", typeof(IMidLevelDBTransactionInternal),
                                                FieldAttributes.Public);
            foreach (var pi in properties)
            {
                var fieldBuilder = tb.DefineField("Field_" + pi.Name, pi.PropertyType, FieldAttributes.Public);
                var getMethodBuilder = tb.DefineMethod("get_" + pi.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, pi.PropertyType, Type.EmptyTypes);
                var ilGenerator = getMethodBuilder.GetILGenerator(16);
                ilGenerator.MarkSequencePoint(symbolDocumentWriter, 1, 1, 1, 1);
                ilGenerator.Emit(OpCodes.Ldarg_0);
                ilGenerator.Emit(OpCodes.Ldfld, fieldBuilder);
                ilGenerator.Emit(OpCodes.Ret);
                tb.DefineMethodOverride(getMethodBuilder, pi.GetGetMethod());
                var setMethodBuilder = tb.DefineMethod("set_" + pi.Name, MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName, typeof(void),
                                                       new[] { pi.PropertyType });
                ilGenerator = setMethodBuilder.GetILGenerator();
                ilGenerator.MarkSequencePoint(symbolDocumentWriter, 1, 1, 1, 1);
                var labelNoChange = ilGenerator.DefineLabel();
                GenerateJumpIfEqual(ilGenerator, pi.PropertyType, labelNoChange,
                                    g =>
                                    {
                                        g.Emit(OpCodes.Ldarg_0);
                                        g.Emit(OpCodes.Ldfld, fieldBuilder);
                                    },
                                    g => g.Emit(OpCodes.Ldarg_1));
                ilGenerator.Emit(OpCodes.Ldarg_0);
                ilGenerator.Emit(OpCodes.Ldarg_1);
                ilGenerator.Emit(OpCodes.Stfld, fieldBuilder);
                ilGenerator.Emit(OpCodes.Ldarg_0);
                ilGenerator.Emit(OpCodes.Ldstr, pi.Name);
                ilGenerator.Emit(OpCodes.Call, raiseINPCMethod);
                ilGenerator.MarkLabel(labelNoChange);
                ilGenerator.Emit(OpCodes.Ret);
                tb.DefineMethodOverride(setMethodBuilder, pi.GetSetMethod());
                var propertyBuilder = tb.DefineProperty(pi.Name, PropertyAttributes.None, pi.PropertyType, Type.EmptyTypes);
                propertyBuilder.SetGetMethod(getMethodBuilder);
                propertyBuilder.SetSetMethod(setMethodBuilder);
            }
            var constructorBuilder = tb.DefineConstructor(MethodAttributes.Family, CallingConventions.Standard,
                                                          new[] { typeof(ulong), typeof(IMidLevelDBTransactionInternal) });
            var ilg = constructorBuilder.GetILGenerator();
            ilg.Emit(OpCodes.Ldarg_0);
            ilg.Emit(OpCodes.Ldarg_1);
            ilg.Emit(OpCodes.Stfld, oidFieldBuilder);
            ilg.Emit(OpCodes.Ldarg_0);
            ilg.Emit(OpCodes.Ldarg_2);
            ilg.Emit(OpCodes.Stfld, trFieldBuilder);
            ilg.Emit(OpCodes.Ret);
            var metbCi = tb.DefineMethod("CreateInstance",
                MethodAttributes.Public | MethodAttributes.Static, typeof(object), new[] { typeof(ulong), typeof(IMidLevelDBTransactionInternal) });
            ilg = metbCi.GetILGenerator();
            ilg.Emit(OpCodes.Ldarg_0);
            ilg.Emit(OpCodes.Ldarg_1);
            ilg.Emit(OpCodes.Newobj, constructorBuilder);
            ilg.Emit(OpCodes.Ret);
            var metb = tb.DefineMethod("Inserter",
                            MethodAttributes.Public | MethodAttributes.Static, typeof(object), new[] { typeof(IMidLevelDBTransactionInternal) });
            ilg = metb.GetILGenerator();
            ilg.DeclareLocal(typeof(object));
            ilg.DeclareLocal(typeof(ulong));
            ilg.Emit(OpCodes.Ldarg_0);
            ilg.Emit(OpCodes.Call, typeof(IMidLevelDBTransactionInternal).GetMethod("CreateNewObjectId"));
            ilg.Emit(OpCodes.Stloc_1);
            ilg.Emit(OpCodes.Ldloc_1);
            ilg.Emit(OpCodes.Ldarg_0);
            ilg.Emit(OpCodes.Call, metbCi);
            ilg.Emit(OpCodes.Stloc_0);
            ilg.Emit(OpCodes.Ldarg_0);
            ilg.Emit(OpCodes.Ldloc_1);
            ilg.Emit(OpCodes.Ldloc_0);
            ilg.Emit(OpCodes.Call, typeof(IMidLevelDBTransactionInternal).GetMethod("RegisterDirtyObject"));
            ilg.Emit(OpCodes.Ldloc_0);
            ilg.Emit(OpCodes.Ret);
            Type result = tb.CreateType();
            ab.Save(name + "asm.dll");
            return result;
        }

        static void GenerateJumpIfEqual(ILGenerator ilGenerator, Type type, Label jumpTo, Action<ILGenerator> loadLeft, Action<ILGenerator> loadRight)
        {
            if (type == typeof(int) || type == typeof(uint) || type == typeof(long) || type == typeof(double))
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
            throw new NotImplementedException(string.Format("Don't know how to compare type {0}", type));
        }

        static MethodInfo GetMethodInfo(Expression<Action> expression)
        {
            return (expression.Body as MethodCallExpression).Method;
        }

        static MethodBuilder GenerateINotifyPropertyChangedImpl(TypeBuilder typeBuilder, ISymbolDocumentWriter symbolDocumentWriter)
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
            ilGenerator.Emit(OpCodes.Call, GetMethodInfo(() => System.Threading.Interlocked.CompareExchange(ref stub, null, null)));
            ilGenerator.Emit(OpCodes.Stloc_0);
            ilGenerator.Emit(OpCodes.Ldloc_0);
            ilGenerator.Emit(OpCodes.Ldloc_1);
            ilGenerator.Emit(OpCodes.Bne_Un_S, label);
            ilGenerator.Emit(OpCodes.Ret);
            typeBuilder.DefineMethodOverride(methodBuilder, add ? eventPropertyChanged.GetAddMethod() : eventPropertyChanged.GetRemoveMethod());
            return methodBuilder;
        }

        internal Func<IMidLevelDBTransactionInternal, object> Inserter
        {
            get
            {
                if (_inserter == null) CreateInserter();
                return _inserter;
            }
        }

        void CreateInserter()
        {
            EnsureImplType();
            var inserter = (Func<IMidLevelDBTransactionInternal, object>)Delegate.CreateDelegate(typeof(Func<IMidLevelDBTransactionInternal, object>), _implType.GetMethod("Inserter"));
            System.Threading.Interlocked.CompareExchange(ref _inserter, inserter, null);
        }

        internal Action<IMidLevelDBTransactionInternal> Saver
        {
            get
            {
                if (_saver == null) CreateSaver();
                return _saver;
            }
        }

        void CreateSaver()
        {
            EnsureImplType();
            throw new NotImplementedException();
        }

        internal void EnsureClientTypeVersion()
        {
            if (_clientTypeVersion != 0) return;
            var props = _clientType.GetProperties();
            var fields = new List<TableFieldInfo>(props.Length);
            foreach (var pi in props)
            {
                if (!pi.CanRead || !pi.CanWrite) continue;
                FieldType ft;
                var pt = pi.PropertyType;
                if (pt == typeof(string))
                {
                    ft = FieldType.String;
                }
                else if (pt == typeof(Byte) || pt == typeof(UInt16) || pt == typeof(UInt32) || pt == typeof(UInt64))
                {
                    ft = FieldType.UInt;
                }
                else if (pt == typeof(SByte) || pt == typeof(Int16) || pt == typeof(Int32) || pt == typeof(Int64))
                {
                    ft = FieldType.Int;
                }
                else
                {
                    throw new BTDBException(string.Format("Type {0} is not supported field type", pt));
                }
                fields.Add(new TableFieldInfo(string.Intern(pi.Name), ft));
            }
            var tvi = new TableVersionInfo(fields.ToArray());
            _tableVersions.TryAdd(_lastVersion + 1, tvi);
            _clientTypeVersion = _lastVersion + 1;
        }
    }
}