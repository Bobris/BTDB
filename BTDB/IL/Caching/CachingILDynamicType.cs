using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace BTDB.IL.Caching
{
    class CachingILDynamicType : IILDynamicType
    {
        readonly CachingILBuilder _cachingIlBuilder;
        readonly string _name;
        readonly Type _baseType;
        readonly Type[] _interfaces;
        readonly List<IReplay> _insts = new List<IReplay>();
        Type TrueContent { get; set; }

        interface IReplay
        {
            void ReplayTo(IILDynamicType target);
            void FinishReplay(IILDynamicType target);
            void FreeTemps();
            bool Equals(IReplay other);
            object TrueContent();
        }

        public CachingILDynamicType(CachingILBuilder cachingIlBuilder, string name, Type baseType, Type[] interfaces)
        {
            _cachingIlBuilder = cachingIlBuilder;
            _name = name;
            _baseType = baseType;
            _interfaces = interfaces;
        }

        public IILMethod DefineMethod(string name, Type returns, Type[] parameters,
            MethodAttributes methodAttributes = MethodAttributes.Public)
        {
            var res = new Method(_insts.Count, name, returns, parameters, methodAttributes);
            _insts.Add(res);
            return res;
        }

        class WithSourceShelving
        {
            SourceCodeWriter _sourceCodeWriter;
            SourceCodeWriter.InsertedBlock _block;

            protected SourceCodeWriter.StoredState ShelveSourceDefinitionBegin(IILDynamicType target )
            {
                if ((_sourceCodeWriter = target.TryGetSourceCodeWriter()) != null)
                    return _sourceCodeWriter.ShelveBegin();
                return null;
            }

            protected void ShelveSourceDefinitionEnd(SourceCodeWriter.StoredState state)
            {
                if (_sourceCodeWriter == null) return;
                _block = _sourceCodeWriter.ShelveEnd(state);
            }

            protected void WriteShelvedSourceDefinition()
            {
                _sourceCodeWriter?.InsertShelvedContent(_block);
            }

            protected void CloseSourceScope()
            {
                _sourceCodeWriter?.CloseScope();
                _sourceCodeWriter?.WriteLine("");
            }
        }

        class Method : WithSourceShelving, IReplay, IILMethodPrivate
        {
            readonly int _id;
            readonly string _name;
            readonly Type _returns;
            readonly Type[] _parameters;
            readonly MethodAttributes _methodAttributes;
            readonly CachingILGen _ilGen = new CachingILGen();
            int _expectedLength = -1;
            IILMethodPrivate _trueContent;

            public Method(int id, string name, Type returns, Type[] parameters, MethodAttributes methodAttributes)
            {
                _id = id;
                _name = name;
                _returns = returns;
                _parameters = parameters;
                _methodAttributes = methodAttributes;
            }

            public void ReplayTo(IILDynamicType target)
            {
                var state = ShelveSourceDefinitionBegin(target);
                _trueContent = (IILMethodPrivate) target.DefineMethod(_name, _returns, _parameters, _methodAttributes);
                if (_expectedLength >= 0) _trueContent.ExpectedLength(_expectedLength);
                ShelveSourceDefinitionEnd(state);
            }

            public void FinishReplay(IILDynamicType target)
            {
                WriteShelvedSourceDefinition();
                _ilGen.ReplayTo(_trueContent.Generator);
                CloseSourceScope();
            }

            public void FreeTemps()
            {
                _trueContent = null;
                _ilGen.FreeTemps();
            }

            public bool Equals(IReplay other)
            {
                var v = other as Method;
                if (v == null) return false;
                return _id == v._id
                    && _name == v._name
                    && _returns == v._returns
                    && _methodAttributes == v._methodAttributes
                    && _parameters.SequenceEqual(v._parameters)
                    && _ilGen.Equals(v._ilGen);
            }

            public override bool Equals(object obj)
            {
                return Equals(obj as IReplay);
            }

            public object TrueContent()
            {
                return _trueContent;
            }

            public void ExpectedLength(int length)
            {
                _expectedLength = length;
            }

            public IILGen Generator => _ilGen;

            public MethodInfo TrueMethodInfo => _trueContent.TrueMethodInfo;

            public Type ReturnType => _returns;

            public Type[] Parameters => _parameters;
        }

        public IILField DefineField(string name, Type type, FieldAttributes fieldAttributes)
        {
            var res = new Field(_insts.Count, name, type, fieldAttributes);
            _insts.Add(res);
            return res;
        }

        class Field : IReplay, IILFieldPrivate
        {
            readonly int _id;
            readonly string _name;
            readonly Type _type;
            readonly FieldAttributes _fieldAttributes;
            IILFieldPrivate _trueContent;

            public Field(int id, string name, Type type, FieldAttributes fieldAttributes)
            {
                _id = id;
                _name = name;
                _type = type;
                _fieldAttributes = fieldAttributes;
            }

            public void ReplayTo(IILDynamicType target)
            {
                _trueContent = (IILFieldPrivate) target.DefineField(_name, _type, _fieldAttributes);
            }

            public void FreeTemps()
            {
                _trueContent = null;
            }

            public bool Equals(IReplay other)
            {
                var v = other as Field;
                if (v == null) return false;
                return _id == v._id && _name == v._name && _type == v._type && _fieldAttributes == v._fieldAttributes;
            }

            public override bool Equals(object obj)
            {
                return Equals(obj as IReplay);
            }

            public object TrueContent()
            {
                return _trueContent;
            }

            public Type FieldType => _type;
            public string Name => _name;
            public void FinishReplay(IILDynamicType target)
            {
            }

            public FieldBuilder TrueField => _trueContent.TrueField;
        }

        public IILEvent DefineEvent(string name, EventAttributes eventAttributes, Type type)
        {
            var res = new Event(_insts.Count, name, eventAttributes, type);
            _insts.Add(res);
            return res;
        }

        class Event : IReplay, IILEvent
        {
            readonly int _id;
            readonly string _name;
            readonly EventAttributes _eventAttributes;
            readonly Type _type;
            IILEvent _trueContent;
            IReplay _addOnMethod;
            IReplay _removeOnMethod;

            public Event(int id, string name, EventAttributes eventAttributes, Type type)
            {
                _id = id;
                _name = name;
                _eventAttributes = eventAttributes;
                _type = type;
            }

            public void ReplayTo(IILDynamicType target)
            {
                _trueContent = target.DefineEvent(_name, _eventAttributes, _type);
            }

            public void FreeTemps()
            {
                _trueContent = null;
            }

            public bool Equals(IReplay other)
            {
                var v = other as Event;
                if (v == null) return false;
                return _id == v._id && _name == v._name && _eventAttributes == v._eventAttributes && _type == v._type;
            }

            public override bool Equals(object obj)
            {
                return Equals(obj as IReplay);
            }

            public object TrueContent()
            {
                return _trueContent;
            }

            public void SetAddOnMethod(IILMethod method)
            {
                _addOnMethod = (IReplay)method;
            }

            public void SetRemoveOnMethod(IILMethod method)
            {
                _removeOnMethod = (IReplay)method;
            }

            public void FinishReplay(IILDynamicType target)
            {
                _trueContent.SetAddOnMethod((IILMethod)_addOnMethod.TrueContent());
                _trueContent.SetRemoveOnMethod((IILMethod)_removeOnMethod.TrueContent());
            }
        }

        public IILMethod DefineConstructor(Type[] parameters)
        {
            var res = new Constructor(_insts.Count, parameters);
            _insts.Add(res);
            return res;
        }

        class Constructor : WithSourceShelving, IReplay, IILMethod
        {
            readonly int _id;
            readonly Type[] _parameters;
            readonly CachingILGen _ilGen = new CachingILGen();
            int _expectedLength = -1;
            IILMethod _trueContent;

            public Constructor(int id, Type[] parameters)
            {
                _id = id;
                _parameters = parameters;
            }

            public void ReplayTo(IILDynamicType target)
            {
                var state = ShelveSourceDefinitionBegin(target);
                _trueContent = target.DefineConstructor(_parameters);
                if (_expectedLength >= 0) _trueContent.ExpectedLength(_expectedLength);
                ShelveSourceDefinitionEnd(state);
            }

            public void FinishReplay(IILDynamicType target)
            {
                WriteShelvedSourceDefinition();
                _ilGen.ReplayTo(_trueContent.Generator);
                CloseSourceScope();
            }

            public void FreeTemps()
            {
                _trueContent = null;
                _ilGen.FreeTemps();
            }

            public bool Equals(IReplay other)
            {
                var v = other as Constructor;
                if (v == null) return false;
                return _id == v._id && _parameters.SequenceEqual(v._parameters) && _ilGen.Equals(v._ilGen);
            }

            public override bool Equals(object obj)
            {
                return Equals(obj as IReplay);
            }

            public object TrueContent()
            {
                return _trueContent;
            }

            public void ExpectedLength(int length)
            {
                _expectedLength = length;
            }

            public IILGen Generator => _ilGen;
        }

        public void DefineMethodOverride(IILMethod methodBuilder, MethodInfo baseMethod)
        {
            _insts.Add(new MethodOverride(_insts.Count, methodBuilder, baseMethod));
        }

        class MethodOverride : IReplay
        {
            readonly int _id;
            readonly IReplay _methodBuilder;
            readonly MethodInfo _baseMethod;

            public MethodOverride(int id, IILMethod methodBuilder, MethodInfo baseMethod)
            {
                _id = id;
                _methodBuilder = (IReplay)methodBuilder;
                _baseMethod = baseMethod;
            }

            public void ReplayTo(IILDynamicType target)
            {
            }

            public void FinishReplay(IILDynamicType target)
            {
                target.DefineMethodOverride((IILMethod)_methodBuilder.TrueContent(), _baseMethod);
            }

            public void FreeTemps()
            {
            }

            public bool Equals(IReplay other)
            {
                var v = other as MethodOverride;
                if (v == null) return false;
                return _id == v._id && _methodBuilder.Equals(v._methodBuilder) && _baseMethod == v._baseMethod;
            }

            public override bool Equals(object obj)
            {
                return Equals(obj as IReplay);
            }

            public object TrueContent()
            {
                throw new InvalidOperationException();
            }
        }

        public Type CreateType()
        {
            lock (_cachingIlBuilder.Lock)
            {
                var item = (CachingILDynamicType)_cachingIlBuilder.FindInCache(this);
                if (item.TrueContent == null)
                {
                    var typeGen = _cachingIlBuilder.Wrapping.NewType(_name, _baseType, _interfaces);
                    _insts.ForEach(r => r.ReplayTo(typeGen));
                    _insts.ForEach(r => r.FinishReplay(typeGen));
                    _insts.ForEach(r => r.FreeTemps());
                    item.TrueContent = typeGen.CreateType();
                }
                return item.TrueContent;
            }
        }

        public SourceCodeWriter TryGetSourceCodeWriter()
        {
            return null;
        }

        public override int GetHashCode()
        {
            return _name.GetHashCode() * 33 + _insts.Count;
        }

        public override bool Equals(object obj)
        {
            var v = obj as CachingILDynamicType;
            if (v == null) return false;
            return _name == v._name && _baseType == v._baseType && _interfaces.SequenceEqual(v._interfaces) &&
                   _insts.SequenceEqual(v._insts, ReplayComparer.Instance);
        }

        class ReplayComparer : IEqualityComparer<IReplay>
        {
            internal static readonly ReplayComparer Instance = new ReplayComparer();

            public bool Equals(IReplay x, IReplay y)
            {
                if (x == y) return true;
                if (x == null) return false;
                return x.Equals(y);
            }

            public int GetHashCode(IReplay obj)
            {
                return 0;
            }
        }
    }
}