using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    internal class ObjectTypeDescriptor : ITypeDescriptor, IPersistTypeDescriptor, ITypeBinarySerializerGenerator
    {
        readonly TypeSerializers _typeSerializers;
        Type _type;
        string _name;
        readonly List<KeyValuePair<string, ITypeDescriptor>> _fields = new List<KeyValuePair<string, ITypeDescriptor>>();

        public ObjectTypeDescriptor(TypeSerializers typeSerializers, Type type)
        {
            _typeSerializers = typeSerializers;
            _type = type;
            Sealed = _type.IsSealed;
            _name = type.FullName;
        }

        public bool Equals(ITypeDescriptor other)
        {
            return Equals(other, new HashSet<ITypeDescriptor>());
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

        public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack)
        {
            if (stack.Contains(this))
            {
                text.Append(Name);
                return;
            }
            stack.Add(this);
            text.Append(Name);
            text.AppendLine(" {");

            text.Append("}");
            stack.Remove(this);
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
            return new Deserializer(this, target);
        }

        internal class Deserializer : ITypeBinaryDeserializerGenerator
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
                return false;
            }

            public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
            {
                var resultLoc = ilGenerator.DeclareLocal(_target, "result");
                ilGenerator
                    .Newobj(_target.GetConstructor(Type.EmptyTypes))
                    .Stloc(resultLoc);
                var props = _target.GetProperties();
                foreach (var pair in _objectTypeDescriptor._fields)
                {
                    var prop = props.FirstOrDefault(p => p.Name == pair.Key);
                    if (prop == null) continue;
                    var des = pair.Value.BuildBinaryDeserializerGenerator(prop.PropertyType);
                    ilGenerator.Ldloc(resultLoc);
                    des.GenerateLoad(ilGenerator, pushReader, pushCtx);
                    ilGenerator.Callvirt(prop.GetSetMethod());
                }
                ilGenerator.Ldloc(resultLoc);
            }
        }

        public ITypeBinarySkipperGenerator BuildBinarySkipperGenerator()
        {
            throw new NotImplementedException();
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

        internal class TypeNewDescriptorGenerator : ITypeNewDescriptorGenerator
        {
            readonly ObjectTypeDescriptor _objectTypeDescriptor;

            public TypeNewDescriptorGenerator(ObjectTypeDescriptor objectTypeDescriptor)
            {
                _objectTypeDescriptor = objectTypeDescriptor;
            }

            public void GenerateTypeIterator(IILGen ilGenerator, Action<IILGen> pushCtx)
            {
                throw new NotImplementedException();
            }
        }

        public IEnumerable<ITypeDescriptor> NestedTypes()
        {
            foreach (var pair in _fields)
            {
                yield return pair.Value;
            }
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

        public void ClearMappingToType()
        {
            _type = null;
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
            return false;
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
                var generator = pair.Value.BuildBinarySerializerGenerator();
                generator.GenerateSave(ilGenerator, pushWriter, pushCtx, il =>
                    {
                        il.Ldloc(locValue);
                        il.Callvirt(_type.GetProperty(pair.Key).GetGetMethod());
                    });
            }
        }
    }
}