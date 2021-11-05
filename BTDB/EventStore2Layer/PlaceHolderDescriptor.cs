using System;
using System.Collections.Generic;
using System.Text;
using BTDB.EventStoreLayer;
using BTDB.IL;

namespace BTDB.EventStore2Layer;

class PlaceHolderDescriptor : ITypeDescriptor
{
    internal readonly ITypeDescriptor TypeDesc;
    internal readonly int TypeId;

    public PlaceHolderDescriptor(int typeId)
    {
        TypeId = typeId;
    }

    public PlaceHolderDescriptor(ITypeDescriptor typeDesc)
    {
        TypeDesc = typeDesc;
    }

    bool IEquatable<ITypeDescriptor>.Equals(ITypeDescriptor other)
    {
        throw new InvalidOperationException();
    }

    string ITypeDescriptor.Name => "";

    bool ITypeDescriptor.FinishBuildFromType(ITypeDescriptorFactory factory)
    {
        throw new InvalidOperationException();
    }

    void ITypeDescriptor.BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
    {
        throw new InvalidOperationException();
    }

    bool ITypeDescriptor.Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack)
    {
        throw new InvalidOperationException();
    }

    Type ITypeDescriptor.GetPreferredType()
    {
        return null;
    }

    public Type GetPreferredType(Type targetType)
    {
        return null;
    }

    public bool AnyOpNeedsCtx()
    {
        throw new InvalidOperationException();
    }

    public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type targetType)
    {
        throw new InvalidOperationException();
    }

    public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        throw new InvalidOperationException();
    }

    public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue, Type valueType)
    {
        throw new InvalidOperationException();
    }

    ITypeNewDescriptorGenerator ITypeDescriptor.BuildNewDescriptorGenerator()
    {
        throw new InvalidOperationException();
    }

    public ITypeDescriptor NestedType(int index)
    {
        throw new InvalidOperationException();
    }

    void ITypeDescriptor.MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        throw new InvalidOperationException();
    }

    bool ITypeDescriptor.Sealed => false;

    bool ITypeDescriptor.StoredInline => false;

    public bool LoadNeedsHelpWithConversion => false;

    void ITypeDescriptor.ClearMappingToType()
    {
        throw new InvalidOperationException();
    }

    public bool ContainsField(string name)
    {
        throw new InvalidOperationException();
    }

    public IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields => Array.Empty<KeyValuePair<string, ITypeDescriptor>>();

    public ITypeDescriptor CloneAndMapNestedTypes(ITypeDescriptorCallbacks typeSerializers, Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        throw new InvalidOperationException();
    }
}
