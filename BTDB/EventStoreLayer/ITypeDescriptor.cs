using System;
using System.Collections.Generic;
using System.Text;

namespace BTDB.EventStoreLayer
{
    public interface ITypeDescriptor : IEquatable<ITypeDescriptor>
    {
        string Name { get; }
        void FinishBuildFromType(ITypeDescriptorFactory factory);
        void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack);
        bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack);
        Type GetPreferedType();
        ITypeBinaryDeserializerGenerator BuildBinaryDeserializerGenerator(Type target);
        ITypeBinarySkipperGenerator BuildBinarySkipperGenerator();
        ITypeBinarySerializerGenerator BuildBinarySerializerGenerator();
        ITypeNewDescriptorGenerator BuildNewDescriptorGenerator();
        IEnumerable<ITypeDescriptor> NestedTypes();
        void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map);
        bool Sealed { get; }
        void ClearMappingToType();
    }
}