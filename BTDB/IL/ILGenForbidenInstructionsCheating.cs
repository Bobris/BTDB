using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILGenForbidenInstructionsCheating : IILGenForbidenInstructions
    {
        class Call : IEquatable<Call>
        {
            readonly OpCode _opCode;
            readonly MethodInfo _methodInfo;
            string _name;
            Type _delegateType;
            static readonly Type[][] GenericDelegates = new[]
                {
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
            }

            public void GenerateStaticParts(TypeBuilder typeBuilder)
            {
                _name = string.Format("{0}_{1}", _opCode, _methodInfo);
                var isFunction = _methodInfo.ReturnType != typeof(void);
                var genDelType = GenericDelegates[isFunction ? 1 : 0][(_methodInfo.IsStatic ? 0 : 1) + _methodInfo.GetParameters().Length];
                var paramTypes = new List<Type>();
                if (!_methodInfo.IsStatic) paramTypes.Add(_methodInfo.DeclaringType);
                paramTypes.AddRange(_methodInfo.GetParameters().Select(p => p.ParameterType));
                var paramTypesWithoutResult = paramTypes.ToArray();
                if (isFunction) paramTypes.Add(_methodInfo.ReturnType);
                _delegateType = genDelType.MakeGenericType(paramTypes.ToArray());
                _fieldBuilder = typeBuilder.DefineField("_" + _name, _delegateType, FieldAttributes.Public | FieldAttributes.Static);
                _staticMethod = typeBuilder.DefineMethod(_name, MethodAttributes.Private | MethodAttributes.Static, _methodInfo.ReturnType, paramTypesWithoutResult);
                var ilGen = new ILGenImpl(_staticMethod.GetILGenerator(), new ILGenForbidenInstructionsGodPowers());
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
                else throw new InvalidOperationException();
                il.Ret();
                finalType.GetField("_" + _name, BindingFlags.Public | BindingFlags.Static).SetValue(null, method.Create());
            }

            public bool Equals(Call other)
            {
                return _opCode == other._opCode && _methodInfo == other._methodInfo;
            }

            public override int GetHashCode()
            {
                return _opCode.GetHashCode() * 33 + _methodInfo.GetHashCode();
            }
        }

        readonly TypeBuilder _typeBuilder;
        readonly HashSet<Call> _calls = new HashSet<Call>();

        public ILGenForbidenInstructionsCheating(TypeBuilder typeBuilder)
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
                    call.EmitCheat(ilGen);
                    return;
                }
            }
            ilGen.Emit(opCode, methodInfo);
        }

        public void FinishType(Type finalType)
        {
            foreach (var call in _calls)
            {
                call.FinishType(finalType);
            }
        }
    }
}