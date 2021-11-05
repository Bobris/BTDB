using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using BTDB.KVDBLayer;

namespace BTDB.IL;

class IilGenForbiddenInstructionsCheating : IILGenForbiddenInstructions
{
    class Call : IEquatable<Call>
    {
        readonly OpCode _opCode;
        readonly MethodInfo _methodInfo;
        readonly ConstructorInfo _constructorInfo;
        string _name;
        Type _delegateType;
        static readonly Type[][] GenericDelegates = {
                new[] { typeof(Action), typeof(Action<>), typeof(Action<,>), typeof(Action<,,>), typeof(Action<,,,>), typeof(Action<,,,,>)},
                new[] { typeof(Func<>), typeof(Func<,>), typeof(Func<,,>), typeof(Func<,,,>), typeof(Func<,,,,>), typeof(Func<,,,,,>)}
            };

        FieldBuilder _fieldBuilder;
        MethodBuilder _staticMethod;
        int _parametersCount;

        public Call(OpCode opCode, MethodInfo methodInfo)
        {
            _opCode = opCode;
            _methodInfo = methodInfo;
            _constructorInfo = null;
        }

        public Call(OpCode opCode, ConstructorInfo constructorInfo)
        {
            _opCode = opCode;
            _methodInfo = null;
            _constructorInfo = constructorInfo;
        }

        Type GetDelegateType(bool isFunction, int numberOfParameters)
        {
            var delegates = GenericDelegates[isFunction ? 1 : 0];
            if (numberOfParameters >= delegates.Length)
            {
                var name = _methodInfo?.Name ?? _constructorInfo.DeclaringType?.Name;
                throw new BTDBException($"Unsupported number of parameters for cheating accessibility in {name} {_name}");
            }
            return delegates[numberOfParameters];
        }

        public void GenerateStaticParts(TypeBuilder typeBuilder)
        {
            _name = $"{_opCode}_{(object)_methodInfo ?? _constructorInfo}";
            Type[] paramTypesWithoutResult;
            if (_methodInfo != null)
            {
                var isFunction = _methodInfo.ReturnType != typeof(void);
                var genDelType = GetDelegateType(isFunction, (_methodInfo.IsStatic ? 0 : 1) + _methodInfo.GetParameters().Length);
                var paramTypes = new List<Type>();
                if (!_methodInfo.IsStatic) paramTypes.Add(_methodInfo.DeclaringType);
                paramTypes.AddRange(_methodInfo.GetParameters().Select(p => p.ParameterType));
                paramTypesWithoutResult = paramTypes.ToArray();
                if (isFunction) paramTypes.Add(_methodInfo.ReturnType);
                _delegateType = genDelType.MakeGenericType(paramTypes.ToArray());
                _fieldBuilder = typeBuilder.DefineField("_" + _name, _delegateType, FieldAttributes.Private | FieldAttributes.Static);
                _staticMethod = typeBuilder.DefineMethod(_name, MethodAttributes.Private | MethodAttributes.Static, _methodInfo.ReturnType, paramTypesWithoutResult);
            }
            else if (_constructorInfo != null)
            {
                var genDelType = GetDelegateType(true, _constructorInfo.GetParameters().Length);
                var paramTypes = new List<Type>();
                paramTypes.AddRange(_constructorInfo.GetParameters().Select(p => p.ParameterType));
                paramTypesWithoutResult = paramTypes.ToArray();

                var constructorType = _constructorInfo.DeclaringType;
                var accesibleConstructorType = IsPublicReal(constructorType) ? constructorType : typeof(object);
                paramTypes.Add(accesibleConstructorType);
                _delegateType = genDelType.MakeGenericType(paramTypes.ToArray());
                _fieldBuilder = typeBuilder.DefineField("_" + _name, _delegateType, FieldAttributes.Private | FieldAttributes.Static);
                _staticMethod = typeBuilder.DefineMethod(_name, MethodAttributes.Private | MethodAttributes.Static, accesibleConstructorType, paramTypesWithoutResult);
            }
            else throw new InvalidOperationException();
            var ilGen = new ILGenImpl(_staticMethod.GetILGenerator(), new IilGenForbiddenInstructionsGodPowers());
            ilGen.Ldsfld(_fieldBuilder);
            _parametersCount = paramTypesWithoutResult.Length;
            for (ushort i = 0; i < _parametersCount; i++)
            {
                ilGen.Ldarg(i);
            }
            ilGen.Call(_delegateType.GetMethod("Invoke"));
            ilGen.Ret();
        }

        public void EmitCheat(ILGenerator ilGen)
        {
            ilGen.Emit(OpCodes.Call, _staticMethod);
        }

        public void FinishType(Type finalType)
        {
            var method = new ILDynamicMethodImpl("proxy", _delegateType, null);
            var il = method.Generator;
            for (ushort i = 0; i < _parametersCount; i++)
            {
                il.Ldarg(i);
            }
            if (_opCode == OpCodes.Call) il.Call(_methodInfo);
            else if (_opCode == OpCodes.Callvirt) il.Callvirt(_methodInfo);
            else if (_opCode == OpCodes.Newobj) il.Newobj(_constructorInfo);
            else throw new InvalidOperationException();
            il.Ret();
            finalType.GetField("_" + _name, BindingFlags.NonPublic | BindingFlags.Static).SetValue(null, method.Create());
        }

        public bool Equals(Call other)
        {
            return _opCode == other._opCode && _methodInfo == other._methodInfo && _constructorInfo == other._constructorInfo;
        }

        public override int GetHashCode()
        {
            if (_methodInfo != null)
                return _opCode.GetHashCode() * 33 + _methodInfo.GetHashCode() * 2;
            return _opCode.GetHashCode() * 33 + _constructorInfo.GetHashCode() * 2 + 1;
        }
    }

    readonly TypeBuilder _typeBuilder;
    readonly HashSet<Call> _calls = new HashSet<Call>();

    public IilGenForbiddenInstructionsCheating(TypeBuilder typeBuilder)
    {
        _typeBuilder = typeBuilder;
    }

    public void Emit(ILGenerator ilGen, OpCode opCode, MethodInfo methodInfo)
    {
        if (opCode == OpCodes.Callvirt || opCode == OpCodes.Call)
        {
            if (!methodInfo.IsPublic && !methodInfo.IsStatic)
            {
                var call = new Call(opCode, methodInfo);
                if (!_calls.Contains(call))
                {
                    _calls.Add(call);
                    call.GenerateStaticParts(_typeBuilder);
                }
                else
                {
                    call = _calls.First(c => c.Equals(call));
                }
                call.EmitCheat(ilGen);
                return;
            }
        }
        ilGen.Emit(opCode, methodInfo);
    }

    public void Emit(ILGenerator ilGen, OpCode opCode, ConstructorInfo constructorInfo)
    {
        if (opCode == OpCodes.Newobj)
        {
            if (!constructorInfo.IsPublic || !IsPublicReal(constructorInfo.DeclaringType))
            {
                var call = new Call(opCode, constructorInfo);
                if (!_calls.Contains(call))
                {
                    _calls.Add(call);
                    call.GenerateStaticParts(_typeBuilder);
                }
                else
                {
                    call = _calls.First(c => c.Equals(call));
                }
                call.EmitCheat(ilGen);
                return;
            }
        }
        ilGen.Emit(opCode, constructorInfo);
    }

    public void FinishType(Type finalType)
    {
        foreach (var call in _calls)
        {
            call.FinishType(finalType);
        }
    }

    internal static bool IsPublicReal(Type constructorType)
    {
        return constructorType.IsPublic || constructorType.IsNestedPublic && IsPublicReal(constructorType.DeclaringType);
    }
}
