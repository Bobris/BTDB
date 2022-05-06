using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using BTDB.IL;

namespace BTDB.EventStoreLayer;

public class SimpleTypeDescriptor : ITypeDescriptor
{
    readonly string _name;
    readonly MethodInfo _loader;
    readonly MethodInfo _skipper;
    readonly MethodInfo _saver;

    public SimpleTypeDescriptor(string name, MethodInfo loader, MethodInfo skipper, MethodInfo saver)
    {
        _name = name;
        _loader = loader;
        _skipper = skipper;
        _saver = saver;
    }

    public string Name => _name;

    public bool FinishBuildFromType(ITypeDescriptorFactory factory)
    {
        return true;
    }

    public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
    {
        text.Append(Name);
    }

    public bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack)
    {
        return ReferenceEquals(this, other);
    }

    public Type GetPreferredType()
    {
        return _loader.ReturnType;
    }

    public Type GetPreferredType(Type targetType)
    {
        return GetPreferredType();
    }

    public ITypeNewDescriptorGenerator BuildNewDescriptorGenerator()
    {
        return null;
    }

    public ITypeDescriptor NestedType(int index)
    {
        return null;
    }

    public void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
    {
    }

    public bool Sealed => true;

    public bool StoredInline => true;

    public bool LoadNeedsHelpWithConversion => true;

    public void ClearMappingToType()
    {
    }

    public bool ContainsField(string name)
    {
        return false;
    }

    public IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields => Array.Empty<KeyValuePair<string, ITypeDescriptor>>();

    public bool AnyOpNeedsCtx()
    {
        return false;
    }

    public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type targetType)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(_loader);
        if (targetType != typeof(object))
        {
            if (targetType != GetPreferredType())
                throw new ArgumentOutOfRangeException(nameof(targetType));
            return;
        }
        if (GetPreferredType().IsValueType)
        {
            ilGenerator.Box(GetPreferredType());
        }
        else
        {
            ilGenerator.Castclass(typeof(object));
        }
    }

    public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(_skipper);
    }

    public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue, Type valueType)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Call(_saver);
    }

    public bool Equals(ITypeDescriptor other)
    {
        return ReferenceEquals(this, other);
    }

    public override int GetHashCode()
    {
        return Name.GetHashCode();
    }

    public ITypeDescriptor CloneAndMapNestedTypes(ITypeDescriptorCallbacks typeSerializers, Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        return this;
    }
}
