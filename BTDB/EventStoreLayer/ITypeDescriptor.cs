using System;
using System.Collections.Generic;
using System.Text;
using BTDB.IL;

namespace BTDB.EventStoreLayer
{
    public interface ITypeDescriptor : IEquatable<ITypeDescriptor>
    {
        string Name { get; }
        bool FinishBuildFromType(ITypeDescriptorFactory factory);
        void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent);
        bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack);
        Type GetPreferedType();
        bool AnyOpNeedsCtx();
        // ctx is ITypeBinaryDeserializerContext
        void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type targetType);
        void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx);
        // ctx is ITypeBinarySerializerContext
        void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue, Type valueType);
        ITypeNewDescriptorGenerator BuildNewDescriptorGenerator();
        ITypeDescriptor NestedType(int index);
        void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map);
        bool Sealed { get; }
        bool StoredInline { get; }
        bool LoadNeedsHelpWithConversion { get; }
        void ClearMappingToType();
        bool ContainsField(string name);
    }
}