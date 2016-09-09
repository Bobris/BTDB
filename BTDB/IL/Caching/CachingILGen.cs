using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL.Caching
{
    class CachingILGen : IILGen
    {
        interface IReplayILGen
        {
            void ReplayTo(IILGen target);
            void FreeTemps();
            bool Equals(IReplayILGen other);
        }

        readonly List<IReplayILGen> _instructions = new List<IReplayILGen>();
        int _lastLocalIndex;
        int _lastLabelIndex;

        internal void ReplayTo(IILGen target)
        {
            foreach (var inst in _instructions)
            {
                inst.ReplayTo(target);
            }
        }

        public override bool Equals(object obj)
        {
            var v = obj as CachingILGen;
            if (v == null) return false;
            if (_instructions.Count != v._instructions.Count) return false;
            for (int i = 0; i < _instructions.Count; i++)
            {
                if (!_instructions[i].Equals(v._instructions[i])) return false;
            }
            return true;
        }

        public override int GetHashCode()
        {
            return _instructions.Count;
        }

        public IILLabel DefineLabel(string name = null)
        {
            var result = new ILLabel(name, _lastLabelIndex);
            _lastLabelIndex++;
            _instructions.Add(result);
            return result;
        }

        class ILLabel : IILLabel, IReplayILGen
        {
            readonly string _name;
            internal readonly int Index;
            internal IILLabel Label;

            public ILLabel(string name, int index)
            {
                _name = name;
                Index = index;
            }

            public void ReplayTo(IILGen target)
            {
                Label = target.DefineLabel(_name);
            }

            public void FreeTemps()
            {
                Label = null;
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as ILLabel;
                if (v==null) return false;
                return _name == v._name;
            }
        }

        public IILLocal DeclareLocal(Type type, string name = null, bool pinned = false)
        {
            var result = new ILLocal(type, name, pinned, _lastLocalIndex);
            _lastLocalIndex++;
            _instructions.Add(result);
            return result;
        }

        class ILLocal : IILLocal, IReplayILGen
        {
            readonly Type _type;
            readonly string _name;
            readonly bool _pinned;
            readonly int _index;
            internal IILLocal Local;

            public ILLocal(Type type, string name, bool pinned, int index)
            {
                _type = type;
                _name = name;
                _pinned = pinned;
                _index = index;
            }

            public int Index => _index;

            public bool Pinned => _pinned;

            public Type LocalType => _type;

            public void ReplayTo(IILGen target)
            {
                Local = target.DeclareLocal(_type, _name, _pinned);
                Debug.Assert(Local.Index == Index);
            }

            public void FreeTemps()
            {
                Local = null;
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as ILLocal;
                if (v == null) return false;
                if (_name != v._name) return false;
                if (_type != v._type) return false;
                return _pinned == v._pinned;
            }
        }

        public IILGen Comment(string text)
        {
            _instructions.Add(new CommentInst(text));
            return this;
        }

        class CommentInst : IReplayILGen
        {
            readonly string _text;

            public CommentInst(string text)
            {
                _text = text;
            }

            public void ReplayTo(IILGen target)
            {
                target.Comment(_text);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as CommentInst;
                if (v == null) return false;
                return _text == v._text;
            }
        }

        public IILGen Mark(IILLabel label)
        {
            _instructions.Add(new MarkInst(label));
            return this;
        }

        class MarkInst : IReplayILGen
        {
            readonly IILLabel _label;

            public MarkInst(IILLabel label)
            {
                _label = label;
            }

            public void ReplayTo(IILGen target)
            {
                target.Mark(((ILLabel)_label).Label);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as MarkInst;
                if (v == null) return false;
                return ((ILLabel) _label).Index == ((ILLabel) v._label).Index;
            }
        }

        public IILGen Ldftn(IILMethod method)
        {
            _instructions.Add(new LdftnInst(method));
            return this;
        }

        class LdftnInst : IReplayILGen
        {
            readonly IILMethod _method;

            public LdftnInst(IILMethod method)
            {
                _method = method;
            }

            public void ReplayTo(IILGen target)
            {
                target.Ldftn(_method);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as LdftnInst;
                if (v == null) return false;
                return _method == v._method;
            }
        }

        public IILGen Ldstr(string str)
        {
            _instructions.Add(new LdstrInst(str));
            return this;
        }

        class LdstrInst : IReplayILGen
        {
            readonly string _str;

            public LdstrInst(string str)
            {
                _str = str;
            }

            public void ReplayTo(IILGen target)
            {
                target.Ldstr(_str);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as LdstrInst;
                if (v == null) return false;
                return _str == v._str;
            }
        }

        public IILGen Try()
        {
            _instructions.Add(new TryInst());
            return this;
        }

        class TryInst : IReplayILGen
        {
            public void ReplayTo(IILGen target)
            {
                target.Try();
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as TryInst;
                return v != null;
            }
        }

        public IILGen Catch(Type exceptionType)
        {
            _instructions.Add(new CatchInst(exceptionType));
            return this;
        }

        class CatchInst : IReplayILGen
        {
            readonly Type _exceptionType;

            public CatchInst(Type exceptionType)
            {
                _exceptionType = exceptionType;
            }

            public void ReplayTo(IILGen target)
            {
                target.Catch(_exceptionType);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as CatchInst;
                if (v == null) return false;
                return _exceptionType == v._exceptionType;
            }
        }

        public IILGen Finally()
        {
            _instructions.Add(new FinallyInst());
            return this;
        }

        class FinallyInst : IReplayILGen
        {
            public void ReplayTo(IILGen target)
            {
                target.Finally();
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as FinallyInst;
                return v != null;
            }
        }

        public IILGen EndTry()
        {
            _instructions.Add(new EndTryInst());
            return this;
        }

        class EndTryInst : IReplayILGen
        {
            public void ReplayTo(IILGen target)
            {
                target.EndTry();
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as EndTryInst;
                return v != null;
            }
        }

        public void Emit(OpCode opCode)
        {
            _instructions.Add(new EmitInst(opCode));
        }

        class EmitInst : IReplayILGen
        {
            readonly OpCode _opCode;

            public EmitInst(OpCode opCode)
            {
                _opCode = opCode;
            }

            public void ReplayTo(IILGen target)
            {
                target.Emit(_opCode);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as EmitInst;
                if (v == null) return false;
                return _opCode == v._opCode;
            }
        }

        public void Emit(OpCode opCode, sbyte param)
        {
            _instructions.Add(new EmitSbyteInst(opCode, param));
        }

        class EmitSbyteInst : IReplayILGen
        {
            readonly OpCode _opCode;
            readonly sbyte _param;

            public EmitSbyteInst(OpCode opCode, sbyte param)
            {
                _opCode = opCode;
                _param = param;
            }

            public void ReplayTo(IILGen target)
            {
                target.Emit(_opCode, _param);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as EmitSbyteInst;
                if (v == null) return false;
                return _opCode == v._opCode && _param == v._param;
            }
        }

        public void Emit(OpCode opCode, byte param)
        {
            _instructions.Add(new EmitByteInst(opCode, param));
        }

        class EmitByteInst : IReplayILGen
        {
            readonly OpCode _opCode;
            readonly byte _param;

            public EmitByteInst(OpCode opCode, byte param)
            {
                _opCode = opCode;
                _param = param;
            }

            public void ReplayTo(IILGen target)
            {
                target.Emit(_opCode, _param);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as EmitByteInst;
                if (v == null) return false;
                return _opCode == v._opCode && _param == v._param;
            }
        }

        public void Emit(OpCode opCode, ushort param)
        {
            _instructions.Add(new EmitUshortInst(opCode, param));
        }

        class EmitUshortInst : IReplayILGen
        {
            readonly OpCode _opCode;
            readonly ushort _param;

            public EmitUshortInst(OpCode opCode, ushort param)
            {
                _opCode = opCode;
                _param = param;
            }

            public void ReplayTo(IILGen target)
            {
                target.Emit(_opCode, _param);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as EmitUshortInst;
                if (v == null) return false;
                return _opCode == v._opCode && _param == v._param;
            }
        }

        public void Emit(OpCode opCode, int param)
        {
            _instructions.Add(new EmitIntInst(opCode, param));
        }

        class EmitIntInst : IReplayILGen
        {
            readonly OpCode _opCode;
            readonly int _param;

            public EmitIntInst(OpCode opCode, int param)
            {
                _opCode = opCode;
                _param = param;
            }

            public void ReplayTo(IILGen target)
            {
                target.Emit(_opCode, _param);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as EmitIntInst;
                if (v == null) return false;
                return _opCode == v._opCode && _param == v._param;
            }
        }

        public void Emit(OpCode opCode, FieldInfo param)
        {
            _instructions.Add(new EmitFieldInfoInst(opCode, param));
        }

        class EmitFieldInfoInst : IReplayILGen
        {
            readonly OpCode _opCode;
            readonly FieldInfo _fieldInfo;

            public EmitFieldInfoInst(OpCode opCode, FieldInfo fieldInfo)
            {
                _opCode = opCode;
                _fieldInfo = fieldInfo;
            }

            public void ReplayTo(IILGen target)
            {
                target.Emit(_opCode, _fieldInfo);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as EmitFieldInfoInst;
                if (v == null) return false;
                return _opCode == v._opCode && _fieldInfo == v._fieldInfo;
            }
        }

        public void Emit(OpCode opCode, ConstructorInfo param)
        {
            _instructions.Add(new EmitConstructorInfoInst(opCode, param));
        }

        class EmitConstructorInfoInst : IReplayILGen
        {
            readonly OpCode _opCode;
            readonly ConstructorInfo _constructorInfo;

            public EmitConstructorInfoInst(OpCode opCode, ConstructorInfo constructorInfo)
            {
                _opCode = opCode;
                _constructorInfo = constructorInfo;
            }

            public void ReplayTo(IILGen target)
            {
                target.Emit(_opCode, _constructorInfo);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as EmitConstructorInfoInst;
                if (v == null) return false;
                return _opCode == v._opCode && _constructorInfo == v._constructorInfo;
            }
        }

        public void Emit(OpCode opCode, MethodInfo param)
        {
            _instructions.Add(new EmitMethodInfoInst(opCode, param));
        }

        class EmitMethodInfoInst : IReplayILGen
        {
            readonly OpCode _opCode;
            readonly MethodInfo _methodInfo;

            public EmitMethodInfoInst(OpCode opCode, MethodInfo methodInfo)
            {
                _opCode = opCode;
                _methodInfo = methodInfo;
            }

            public void ReplayTo(IILGen target)
            {
                target.Emit(_opCode, _methodInfo);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as EmitMethodInfoInst;
                if (v == null) return false;
                return _opCode == v._opCode && _methodInfo == v._methodInfo;
            }
        }

        public void Emit(OpCode opCode, Type type)
        {
            _instructions.Add(new EmitTypeInst(opCode, type));
        }

        class EmitTypeInst : IReplayILGen
        {
            readonly OpCode _opCode;
            readonly Type _type;

            public EmitTypeInst(OpCode opCode, Type type)
            {
                _opCode = opCode;
                _type = type;
            }

            public void ReplayTo(IILGen target)
            {
                target.Emit(_opCode, _type);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as EmitTypeInst;
                if (v == null) return false;
                return _opCode == v._opCode && _type == v._type;
            }
        }

        public void Emit(OpCode opCode, IILLocal ilLocal)
        {
            _instructions.Add(new EmitILLocal(opCode, ilLocal));
        }

        class EmitILLocal : IReplayILGen
        {
            readonly OpCode _opCode;
            readonly IILLocal _ilLocal;

            public EmitILLocal(OpCode opCode, IILLocal ilLocal)
            {
                _opCode = opCode;
                _ilLocal = ilLocal;
            }

            public void ReplayTo(IILGen target)
            {
                target.Emit(_opCode, ((ILLocal)_ilLocal).Local);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as EmitILLocal;
                if (v == null) return false;
                return _opCode == v._opCode && ((ILLocal)_ilLocal).Index == ((ILLocal)v._ilLocal).Index;
            }
        }

        public void Emit(OpCode opCode, IILLabel ilLabel)
        {
            _instructions.Add(new EmitILLabel(opCode, ilLabel));
        }

        class EmitILLabel : IReplayILGen
        {
            readonly OpCode _opCode;
            readonly IILLabel _ilLabel;

            public EmitILLabel(OpCode opCode, IILLabel ilLabel)
            {
                _opCode = opCode;
                _ilLabel = ilLabel;
            }

            public void ReplayTo(IILGen target)
            {
                target.Emit(_opCode, ((ILLabel)_ilLabel).Label);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplayILGen other)
            {
                var v = other as EmitILLabel;
                if (v == null) return false;
                return _opCode == v._opCode && ((ILLabel)_ilLabel).Index == ((ILLabel)v._ilLabel).Index;
            }
        }

        internal void FreeTemps()
        {
            foreach (var inst in _instructions)
            {
                inst.FreeTemps();
            }
        }
    }
}