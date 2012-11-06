using System;
using System.Collections.Generic;
using System.Text;

namespace BTDB.EventStoreLayer
{
    public interface ITypeDescriptor
    {
        string Name { get; }
        void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack);
        ITypeBinaryDeserializerGenerator BuildBinaryDeserializerGenerator(Type target);
        ITypeBinarySkipperGenerator BuildBinarySkipperGenerator();
        ITypeBinarySerializerGenerator BuildBinarySerializerGenerator();
        ITypeDynamicTypeIterationGenerator BuildDynamicTypeIterationGenerator();
        IEnumerable<ITypeDescriptor> NestedTypes();
    }
}