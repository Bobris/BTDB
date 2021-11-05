using System;
using System.Diagnostics;
using System.Reflection;
using System.Reflection.Emit;
using BTDB.Collections;

namespace BTDB.IL.Caching;

class CachingILGen : IILGen
{
    interface IReplayILGen
    {
        void ReplayTo(IILGen target);
        void FreeTemps();
        bool Equals(IReplayILGen other);
    }

    StructList<IReplayILGen> _instructions;
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
        if (!(obj is CachingILGen v)) return false;
        if (_instructions.Count != v._instructions.Count) return false;
        for (var i = 0; i < _instructions.Count; i++)
        {
            if (!_instructions[i].Equals(v._instructions[i])) return false;
        }
        return true;
    }

    public override int GetHashCode()
    {
        // ReSharper disable once NonReadonlyMemberInGetHashCode
        return (int)_instructions.Count;
    }

    public IILLabel DefineLabel(string? name = null)
    {
        var result = new ILLabel(name, _lastLabelIndex);
        _lastLabelIndex++;
        _instructions.Add(result);
        return result;
    }

    class ILLabel : IILLabel, IReplayILGen
    {
        readonly string? _name;
        internal readonly int Index;
        internal IILLabel? Label;

        public ILLabel(string? name, int index)
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
            if (!(other is ILLabel v)) return false;
            return _name == v._name;
        }
    }

    public IILLocal DeclareLocal(Type type, string? name = null, bool pinned = false)
    {
        var result = new ILLocal(type, name, pinned, _lastLocalIndex);
        _lastLocalIndex++;
        _instructions.Add(result);
        return result;
    }

    class ILLocal : IILLocal, IReplayILGen
    {
        readonly string? _name;
        internal IILLocal? Local;

        public ILLocal(Type type, string? name, bool pinned, int index)
        {
            LocalType = type;
            _name = name;
            Pinned = pinned;
            Index = index;
        }

        public int Index { get; }

        public bool Pinned { get; }

        public Type LocalType { get; }

        public void ReplayTo(IILGen target)
        {
            Local = target.DeclareLocal(LocalType, _name, Pinned);
            Debug.Assert(Local.Index == Index);
        }

        public void FreeTemps()
        {
            Local = null;
        }

        public bool Equals(IReplayILGen other)
        {
            if (!(other is ILLocal v)) return false;
            if (_name != v._name) return false;
            if (LocalType != v.LocalType) return false;
            return Pinned == v.Pinned;
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
            if (!(other is CommentInst v)) return false;
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
            target.Mark(((ILLabel)_label).Label!);
        }

        public void FreeTemps()
        {
        }

        public bool Equals(IReplayILGen other)
        {
            if (!(other is MarkInst v)) return false;
            return ((ILLabel)_label).Index == ((ILLabel)v._label).Index;
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
            if (!(other is LdftnInst v)) return false;
            return ReferenceEquals(_method, v._method);
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
            if (!(other is LdstrInst v)) return false;
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
            return other is TryInst;
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
            if (!(other is CatchInst v)) return false;
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
            return other is FinallyInst;
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
            return other is EndTryInst;
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
            if (!(other is EmitInst v)) return false;
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
            if (!(other is EmitSbyteInst v)) return false;
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
            if (!(other is EmitByteInst v)) return false;
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
            if (!(other is EmitUshortInst v)) return false;
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
            if (!(other is EmitIntInst v)) return false;
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
            if (!(other is EmitFieldInfoInst v)) return false;
            return _opCode == v._opCode && _fieldInfo == v._fieldInfo;
        }
    }

    public void Emit(OpCode opCode, ConstructorInfo param)
    {
        if (param == null) throw new ArgumentNullException(nameof(param));
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
            if (!(other is EmitConstructorInfoInst v)) return false;
            return _opCode == v._opCode && _constructorInfo == v._constructorInfo;
        }
    }

    public void Emit(OpCode opCode, MethodInfo param)
    {
        if (param == null) throw new ArgumentNullException(nameof(param));
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
            if (!(other is EmitMethodInfoInst v)) return false;
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
            if (!(other is EmitTypeInst v)) return false;
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
            target.Emit(_opCode, ((ILLocal)_ilLocal).Local!);
        }

        public void FreeTemps()
        {
        }

        public bool Equals(IReplayILGen other)
        {
            if (!(other is EmitILLocal v)) return false;
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
            target.Emit(_opCode, ((ILLabel)_ilLabel).Label!);
        }

        public void FreeTemps()
        {
        }

        public bool Equals(IReplayILGen other)
        {
            if (!(other is EmitILLabel v)) return false;
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

    public void Emit(OpCode opCode, IILField ilField)
    {
        _instructions.Add(new EmitILField(opCode, ilField));
    }

    class EmitILField : IReplayILGen
    {
        readonly OpCode _opCode;
        readonly IILField _ilField;

        public EmitILField(OpCode opCode, IILField ilField)
        {
            _opCode = opCode;
            _ilField = ilField;
        }

        public void ReplayTo(IILGen target)
        {
            target.Emit(_opCode, _ilField);
        }

        public void FreeTemps()
        {
            ((IILFieldPrivate)_ilField).FreeTemps();
        }

        public bool Equals(IReplayILGen other)
        {
            if (!(other is EmitILField v)) return false;
            return _opCode == v._opCode && _ilField.Equals(v._ilField);
        }
    }

    public void Emit(OpCode opCode, long value)
    {
        _instructions.Add(new EmitLongInst(opCode, value));
    }

    class EmitLongInst : IReplayILGen
    {
        readonly OpCode _opCode;
        readonly long _param;

        public EmitLongInst(OpCode opCode, long param)
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
            if (!(other is EmitLongInst v)) return false;
            return _opCode == v._opCode && _param == v._param;
        }
    }

    public void Emit(OpCode opCode, float value)
    {
        _instructions.Add(new EmitFloatInst(opCode, value));
    }

    class EmitFloatInst : IReplayILGen
    {
        readonly OpCode _opCode;
        readonly float _param;

        public EmitFloatInst(OpCode opCode, float param)
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
            if (!(other is EmitFloatInst v)) return false;
            // ReSharper disable once CompareOfFloatsByEqualityOperator
            return _opCode == v._opCode && _param == v._param;
        }
    }

    public void Emit(OpCode opCode, double value)
    {
        _instructions.Add(new EmitDoubleInst(opCode, value));
    }

    class EmitDoubleInst : IReplayILGen
    {
        readonly OpCode _opCode;
        readonly double _param;

        public EmitDoubleInst(OpCode opCode, double param)
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
            if (!(other is EmitDoubleInst v)) return false;
            // ReSharper disable once CompareOfFloatsByEqualityOperator
            return _opCode == v._opCode && _param == v._param;
        }
    }
}
