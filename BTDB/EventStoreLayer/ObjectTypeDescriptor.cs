using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public class ObjectTypeDescriptor : ITypeDescriptor, IPersistTypeDescriptor, ITypeBinarySerializerGenerator
    {
        Type _type;
        readonly string _name;
        readonly List<KeyValuePair<string, ITypeDescriptor>> _fields = new List<KeyValuePair<string, ITypeDescriptor>>();

        public ObjectTypeDescriptor(TypeSerializers typeSerializers, Type type)
        {
            _type = type;
            Sealed = _type.IsSealed;
            _name = typeSerializers.TypeToName(type);
        }

        public ObjectTypeDescriptor(AbstractBufferedReader reader, Func<AbstractBufferedReader, ITypeDescriptor> nestedDescriptorReader)
        {
            _type = null;
            Sealed = false;
            _name = reader.ReadString();
            var fieldCount = reader.ReadVUInt32();
            while (fieldCount-- > 0)
            {
                _fields.Add(new KeyValuePair<string, ITypeDescriptor>(reader.ReadString(), nestedDescriptorReader(reader)));
            }
        }

        public bool Equals(ITypeDescriptor other)
        {
            return Equals(other, new HashSet<ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance));
        }

        public string Name
        {
            get { return _name; }
        }

        public void FinishBuildFromType(ITypeDescriptorFactory factory)
        {
            var props = _type.GetProperties();
            foreach (var propertyInfo in props)
            {
                var descriptor = factory.Create(propertyInfo.PropertyType);
                if (descriptor != null)
                {
                    _fields.Add(new KeyValuePair<string, ITypeDescriptor>(propertyInfo.Name, descriptor));
                }
            }
        }

        public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
        {
            if (stack.Contains(this))
            {
                text.Append(Name);
                return;
            }
            stack.Add(this);
            text.AppendLine(Name);
            AppendIndent(text, indent);
            text.AppendLine("{");
            indent++;
            foreach (var pair in _fields)
            {
                AppendIndent(text, indent);
                text.Append(pair.Key);
                text.Append(" : ");
                pair.Value.BuildHumanReadableFullName(text, stack, indent);
                text.AppendLine();
            }
            indent--;
            AppendIndent(text, indent);
            text.Append("}");
            stack.Remove(this);
        }

        static void AppendIndent(StringBuilder text, uint indent)
        {
            text.Append(' ', (int)(indent * 4));
        }

        public bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack)
        {
            var o = other as ObjectTypeDescriptor;
            if (o == null) return false;
            if (Name != o.Name) return false;
            if (stack.Contains(this)) return true;
            if (_fields.Count != o._fields.Count) return false;
            stack.Add(this);
            try
            {
                for (int i = 0; i < _fields.Count; i++)
                {
                    if (_fields[i].Key != o._fields[i].Key) return false;
                    if (!_fields[i].Value.Equals(o._fields[i].Value, stack)) return false;
                }
            }
            finally
            {
                stack.Remove(this);
            }
            return true;
        }

        public Type GetPreferedType()
        {
            return _type;
        }

        public ITypeBinaryDeserializerGenerator BuildBinaryDeserializerGenerator(Type target)
        {
            if (target == typeof(object))
            {
                return new DynamicDeserializer(this);
            }
            return new Deserializer(this, target);
        }

        class DynamicDeserializer : ITypeBinaryDeserializerGenerator
        {
            readonly ObjectTypeDescriptor _ownerDescriptor;

            public DynamicDeserializer(ObjectTypeDescriptor ownerDescriptor)
            {
                _ownerDescriptor = ownerDescriptor;
            }

            public bool LoadNeedsCtx()
            {
                return true;
            }

            public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor)
            {
                var resultLoc = ilGenerator.DeclareLocal(typeof(DynamicObject), "result");
                ilGenerator
                    .Do(pushDescriptor)
                    .Castclass(typeof(ObjectTypeDescriptor))
                    .Newobj(typeof(DynamicObject).GetConstructor(new[] { typeof(ObjectTypeDescriptor) }))
                    .Stloc(resultLoc)
                    .Do(pushCtx)
                    .Ldloc(resultLoc)
                    .Callvirt(() => default(ITypeBinaryDeserializerContext).AddBackRef(null));
                var idx = 0;
                foreach (var pair in _ownerDescriptor._fields)
                {
                    var idxForCapture = idx;
                    ilGenerator.Ldloc(resultLoc);
                    ilGenerator.LdcI4(idx);
                    pair.Value.GenerateLoad(ilGenerator, pushReader, pushCtx, il => il.Do(pushDescriptor).LdcI4(idxForCapture).Callvirt(() => default(ITypeDescriptor).NestedType(0)), typeof(object));
                    ilGenerator.Callvirt(() => default(DynamicObject).SetFieldByIdxFast(0, null));
                    idx++;
                }
                ilGenerator
                    .Ldloc(resultLoc)
                    .Castclass(typeof(object));
            }
        }

        int FindFieldIndex(string fieldName)
        {
            return _fields.FindIndex(p => p.Key == fieldName);
        }

        int FindFieldIndexWithThrow(string fieldName)
        {
            var realidx = FindFieldIndex(fieldName);
            if (realidx < 0)
                throw new MemberAccessException(string.Format("{0} does not have member {1}", Name, fieldName));
            return realidx;
        }

        public class DynamicObject : IDynamicMetaObjectProvider, IKnowDescriptor
        {
            readonly ObjectTypeDescriptor _ownerDescriptor;
            readonly object[] _fieldValues;

            public DynamicObject(ObjectTypeDescriptor ownerDescriptor)
            {
                _ownerDescriptor = ownerDescriptor;
                _fieldValues = new object[_ownerDescriptor._fields.Count];
            }

            DynamicMetaObject IDynamicMetaObjectProvider.GetMetaObject(Expression parameter)
            {
                return new DynamicDictionaryMetaObject(parameter, this);
            }

            public void SetFieldByIdxFast(int idx, object value)
            {
                _fieldValues[idx] = value;
            }

            public void SetFieldByIdx(int idx, string fieldName, ObjectTypeDescriptor descriptor, object value)
            {
                if (_ownerDescriptor == descriptor)
                {
                    if (idx < 0)
                        ThrowMemberAccessException(fieldName);
                    _fieldValues[idx] = value;
                    return;
                }
                var realidx = _ownerDescriptor.FindFieldIndexWithThrow(fieldName);
                _fieldValues[realidx] = value;
            }

            public object GetFieldByIdx(int idx, string fieldName, ObjectTypeDescriptor descriptor)
            {
                if (_ownerDescriptor == descriptor)
                {
                    if (idx < 0)
                        ThrowMemberAccessException(fieldName);
                    return _fieldValues[idx];
                }
                var realidx = _ownerDescriptor.FindFieldIndexWithThrow(fieldName);
                return _fieldValues[realidx];
            }

            void ThrowMemberAccessException(string fieldName)
            {
                throw new MemberAccessException(string.Format("{0} does not have member {1}", _ownerDescriptor.Name, fieldName));
            }

            class DynamicDictionaryMetaObject : DynamicMetaObject
            {
                internal DynamicDictionaryMetaObject(Expression parameter, DynamicObject value)
                    : base(parameter, BindingRestrictions.Empty, value)
                {
                }

                public override DynamicMetaObject BindSetMember(SetMemberBinder binder, DynamicMetaObject value)
                {
                    var descriptor = (Value as DynamicObject)._ownerDescriptor;
                    var idx = descriptor._fields.FindIndex(p => p.Key == binder.Name);
                    return new DynamicMetaObject(Expression.Call(Expression.Convert(Expression, LimitType),
                        typeof(DynamicObject).GetMethod("SetFieldByIdx"),
                        Expression.Constant(idx),
                        Expression.Constant(binder.Name),
                        Expression.Constant(descriptor),
                        Expression.Convert(value.Expression, typeof(object))),
                        BindingRestrictions.GetTypeRestriction(Expression, LimitType));
                }

                public override DynamicMetaObject BindGetMember(GetMemberBinder binder)
                {
                    var descriptor = (Value as DynamicObject)._ownerDescriptor;
                    var idx = descriptor._fields.FindIndex(p => p.Key == binder.Name);
                    return new DynamicMetaObject(Expression.Call(Expression.Convert(Expression, LimitType),
                        typeof(DynamicObject).GetMethod("GetFieldByIdx"),
                        Expression.Constant(idx),
                        Expression.Constant(binder.Name),
                        Expression.Constant(descriptor)),
                        BindingRestrictions.GetTypeRestriction(Expression, LimitType));
                }

                public override IEnumerable<string> GetDynamicMemberNames()
                {
                    var descriptor = (Value as DynamicObject)._ownerDescriptor;
                    return descriptor._fields.Select(p => p.Key);
                }
            }

            public override string ToString()
            {
                var sb = new StringBuilder();
                var idx = 0;
                sb.Append("{ ");
                foreach (var item in _ownerDescriptor._fields)
                {
                    if (idx > 0) sb.Append(", ");
                    sb.Append(item.Key).Append(": ").AppendJsonLike(_fieldValues[idx]);
                    idx++;
                }
                sb.Append(" }");
                return sb.ToString();
            }

            public ITypeDescriptor GetDescriptor()
            {
                return _ownerDescriptor;
            }
        }

        class Deserializer : ITypeBinaryDeserializerGenerator
        {
            readonly ObjectTypeDescriptor _objectTypeDescriptor;
            readonly Type _target;

            public Deserializer(ObjectTypeDescriptor objectTypeDescriptor, Type target)
            {
                _objectTypeDescriptor = objectTypeDescriptor;
                _target = target;
            }

            public bool LoadNeedsCtx()
            {
                return !_objectTypeDescriptor._fields.All(p => p.Value.StoredInline) || _objectTypeDescriptor._fields.Any(p => p.Value.BuildBinaryDeserializerGenerator(p.Value.GetPreferedType()).LoadNeedsCtx());
            }

            public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor)
            {
                var resultLoc = ilGenerator.DeclareLocal(_target, "result");
                var labelNoCtx = ilGenerator.DefineLabel();
                ilGenerator
                    .Newobj(_target.GetConstructor(Type.EmptyTypes))
                    .Stloc(resultLoc)
                    .Do(pushCtx)
                    .BrfalseS(labelNoCtx)
                    .Do(pushCtx)
                    .Ldloc(resultLoc)
                    .Callvirt(() => default(ITypeBinaryDeserializerContext).AddBackRef(null))
                    .Mark(labelNoCtx);
                var props = _target.GetProperties();
                for (var idx = 0; idx < _objectTypeDescriptor._fields.Count; idx++)
                {
                    var idxForCapture = idx;
                    var pair = _objectTypeDescriptor._fields[idx];
                    var prop = props.FirstOrDefault(p => p.Name == pair.Key);
                    if (prop == null)
                    {
                        if (pair.Value.StoredInline)
                        {
                            var skipper = pair.Value.BuildBinarySkipperGenerator();
                            skipper.GenerateSkip(ilGenerator, pushReader, pushCtx);
                            continue;
                        }
                        ilGenerator
                            .Do(pushCtx)
                            .Callvirt(() => default(ITypeBinaryDeserializerContext).SkipObject());
                        continue;
                    }
                    ilGenerator.Ldloc(resultLoc);
                    pair.Value.GenerateLoad(ilGenerator, pushReader, pushCtx,
                                            il => il.Do(pushDescriptor).LdcI4(idxForCapture).Callvirt(() => default(ITypeDescriptor).NestedType(0)),
                                            prop.PropertyType);
                    ilGenerator.Callvirt(prop.GetSetMethod());
                }
                ilGenerator.Ldloc(resultLoc);
            }
        }

        public ITypeBinarySkipperGenerator BuildBinarySkipperGenerator()
        {
            throw new InvalidOperationException();
        }

        public ITypeBinarySerializerGenerator BuildBinarySerializerGenerator()
        {
            return this;
        }

        public ITypeNewDescriptorGenerator BuildNewDescriptorGenerator()
        {
            if (_fields.Select(p => p.Value).All(d => d.Sealed)) return null;
            return new TypeNewDescriptorGenerator(this);
        }

        class TypeNewDescriptorGenerator : ITypeNewDescriptorGenerator
        {
            readonly ObjectTypeDescriptor _objectTypeDescriptor;

            public TypeNewDescriptorGenerator(ObjectTypeDescriptor objectTypeDescriptor)
            {
                _objectTypeDescriptor = objectTypeDescriptor;
            }

            public void GenerateTypeIterator(IILGen ilGenerator, Action<IILGen> pushObj, Action<IILGen> pushCtx)
            {
                foreach (var pair in _objectTypeDescriptor._fields)
                {
                    if (pair.Value.Sealed) continue;
                    ilGenerator
                        .Do(pushCtx)
                        .Do(pushObj)
                        .Castclass(_objectTypeDescriptor._type)
                        .Callvirt(_objectTypeDescriptor._type.GetProperty(pair.Key).GetGetMethod())
                        .Callvirt(() => default(IDescriptorSerializerLiteContext).StoreNewDescriptors(null));
                }
            }
        }

        public ITypeDescriptor NestedType(int index)
        {
            if (index < _fields.Count) return _fields[index].Value;
            return null;
        }

        public void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
        {
            for (int index = 0; index < _fields.Count; index++)
            {
                var keyValuePair = _fields[index];
                keyValuePair = new KeyValuePair<string, ITypeDescriptor>(keyValuePair.Key, map(keyValuePair.Value));
                _fields[index] = keyValuePair;
            }
        }

        public bool Sealed { get; private set; }

        public bool StoredInline { get { return false; } }

        public void ClearMappingToType()
        {
            _type = null;
        }

        public bool ContainsField(string name)
        {
            foreach (var pair in _fields)
            {
                if (pair.Key == name) return true;
            }
            return false;
        }

        public void Persist(AbstractBufferedWriter writer, Action<AbstractBufferedWriter, ITypeDescriptor> nestedDescriptorPersistor)
        {
            writer.WriteString(Name);
            writer.WriteVUInt32((uint)_fields.Count);
            foreach (var pair in _fields)
            {
                writer.WriteString(pair.Key);
                nestedDescriptorPersistor(writer, pair.Value);
            }
        }

        public bool SaveNeedsCtx()
        {
            return !_fields.All(p => p.Value.StoredInline) || _fields.Any(p => p.Value.BuildBinarySerializerGenerator().SaveNeedsCtx());
        }

        public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue)
        {
            var locValue = ilGenerator.DeclareLocal(_type, "value");
            ilGenerator
                .Do(pushValue)
                .Stloc(locValue);
            foreach (var pairi in _fields)
            {
                var pair = pairi;
                pair.Value.GenerateSave(ilGenerator, pushWriter, pushCtx, il => il
                    .Ldloc(locValue)
                    .Callvirt(_type.GetProperty(pair.Key).GetGetMethod()));
            }
        }
    }
}