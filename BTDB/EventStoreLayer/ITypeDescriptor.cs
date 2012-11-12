using System;
using System.Collections.Generic;
using System.Text;

namespace BTDB.EventStoreLayer
{
    public interface ITypeDescriptor : IEquatable<ITypeDescriptor>
    {
        string Name { get; }
        void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack);
        bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack);
        Type GetPreferedType();
        ITypeBinaryDeserializerGenerator BuildBinaryDeserializerGenerator(Type target);
        ITypeBinarySkipperGenerator BuildBinarySkipperGenerator();
        ITypeBinarySerializerGenerator BuildBinarySerializerGenerator();
        ITypeNewDescriptorGenerator BuildNewDescriptorGenerator();
        IEnumerable<ITypeDescriptor> NestedTypes();
        bool Sealed { get; }
        void ClearMappingToType();
    }
}