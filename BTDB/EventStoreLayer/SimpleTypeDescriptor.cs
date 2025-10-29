using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public class SimpleTypeDescriptor : ITypeDescriptor
{
    readonly string _name;
    readonly MethodInfo _loader;
    readonly MethodInfo _skipper;
    readonly MethodInfo _saver;
    readonly Layer2Loader _load;
    readonly Layer2Skipper _skip;
    readonly Layer2Saver _save;

    public SimpleTypeDescriptor(string name, MethodInfo loader, MethodInfo skipper, MethodInfo saver, Layer2Loader load,
        Layer2Skipper skip, Layer2Saver save)
    {
        _name = name;
        _loader = loader;
        _skipper = skipper;
        _saver = saver;
        _load = load;
        _skip = skip;
        _save = save;
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

    public bool Equals(ITypeDescriptor other, Dictionary<ITypeDescriptor, ITypeDescriptor>? equalities)
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
        return null!;
    }

    public ITypeDescriptor NestedType(int index)
    {
        return null!;
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

    public IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields =>
        Array.Empty<KeyValuePair<string, ITypeDescriptor>>();

    public bool AnyOpNeedsCtx()
    {
        return false;
    }

    public Layer2Loader GenerateLoad(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        if (targetType == GetPreferredType())
        {
            return _load;
        }

        return this.BuildConvertingLoader(GetPreferredType(), targetType, typeConverterFactory);
    }

    public void Skip(ref MemReader reader, ITypeBinaryDeserializerContext? ctx)
    {
        _skip(ref reader, ctx);
    }

    public Layer2Saver GenerateSave(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        if (targetType == GetPreferredType())
        {
            return _save;
        }

        return this.BuildConvertingSaver(targetType, GetPreferredType(), typeConverterFactory);
    }

    public Layer2NewDescriptor? GenerateNewDescriptor(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        return null;
    }

    public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx,
        Action<IILGen> pushDescriptor, Type targetType)
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

    public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx,
        Action<IILGen> pushValue, Type valueType)
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

    public ITypeDescriptor CloneAndMapNestedTypes(ITypeDescriptorCallbacks typeSerializers,
        Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        return this;
    }
}
