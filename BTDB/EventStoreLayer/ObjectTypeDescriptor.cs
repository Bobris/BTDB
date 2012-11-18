using System;
using System.Collections.Generic;
using System.Text;

namespace BTDB.EventStoreLayer
{
    internal class ObjectTypeDescriptor : ITypeDescriptor
    {
        Type _type;
        string _name;

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
            throw new NotImplementedException();
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
            if (stack.Contains(this)) return true;
            stack.Add(this);
            throw new NotImplementedException();
            stack.Remove(this);
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
            throw new NotImplementedException();
        }

        public bool Sealed { get; private set; }
 
        public void ClearMappingToType()
        {
            _type = null;
        }
    }
}