using System;
using System.Collections.Generic;
using System.Text;

namespace BTDB.EventStoreLayer
{
    internal class ObjectTypeDescriptor : ITypeDescriptor
    {
        Type _type;
        string _name;
        readonly List<KeyValuePair<string, ITypeDescriptor>> _fields = new List<KeyValuePair<string, ITypeDescriptor>>();

        public ObjectTypeDescriptor(Type type)
        {
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
            throw new NotImplementedException();
        }

        public ITypeBinarySkipperGenerator BuildBinarySkipperGenerator()
        {
            throw new NotImplementedException();
        }

        public ITypeBinarySerializerGenerator BuildBinarySerializerGenerator()
        {
            throw new NotImplementedException();
        }

        public ITypeNewDescriptorGenerator BuildNewDescriptorGenerator()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<ITypeDescriptor> NestedTypes()
        {
            throw new NotImplementedException();
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
    }
}