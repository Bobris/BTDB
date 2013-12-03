using System;
using System.Collections.Generic;
using System.Text;
using BTDB.IL;

namespace BTDB.EventStoreLayer
{
    public interface ITypeDescriptor : IEquatable<ITypeDescriptor>
    {
        string Name { get; }
        void FinishBuildFromType(ITypeDescriptorFactory factory);
        void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent);
        bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack);
        Type GetPreferedType();
        bool LoadNeedsCtx();
        // ctx is ITypeBinaryDeserializerContext
        void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type targetType);
        ITypeBinarySkipperGenerator BuildBinarySkipperGenerator();
        ITypeBinarySerializerGenerator BuildBinarySerializerGenerator();
        ITypeNewDescriptorGenerator BuildNewDescriptorGenerator();
        ITypeDescriptor NestedType(int index);
        void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map);
        bool Sealed { get; }
        bool StoredInline { get; }
        void ClearMappingToType();
        bool ContainsField(string name);
    }
}