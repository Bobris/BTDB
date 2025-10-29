using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public class EncryptedStringDescriptor : ITypeDescriptor
{
    public string Name => "EncryptedString";

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

    public Type GetPreferredType() => typeof(EncryptedString);

    public Type GetPreferredType(Type targetType)
    {
        return GetPreferredType();
    }

    public ITypeNewDescriptorGenerator? BuildNewDescriptorGenerator()
    {
        return null;
    }

    public ITypeDescriptor? NestedType(int index)
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

    public IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields =>
        Array.Empty<KeyValuePair<string, ITypeDescriptor>>();

    public bool AnyOpNeedsCtx() => true;

    public Layer2Loader GenerateLoad(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        if (targetType == typeof(EncryptedString) || targetType == typeof(string))
        {
            return (ref MemReader reader, ITypeBinaryDeserializerContext? ctx, ref byte value) =>
            {
                Unsafe.As<byte, EncryptedString>(ref value) = ctx!.LoadEncryptedString(ref reader);
            };
        }

        return this.BuildConvertingLoader(typeof(EncryptedString), targetType, typeConverterFactory);
    }

    public void Skip(ref MemReader reader, ITypeBinaryDeserializerContext? ctx)
    {
        ctx!.SkipEncryptedString(ref reader);
    }

    public Layer2Saver GenerateSave(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        if (targetType == typeof(EncryptedString) || targetType == typeof(string))
        {
            return (ref MemWriter writer, ITypeBinarySerializerContext? ctx, ref byte value) =>
            {
                ctx!.StoreEncryptedString(ref writer, Unsafe.As<byte, EncryptedString>(ref value));
            };
        }

        return this.BuildConvertingSaver(targetType, typeof(EncryptedString), typeConverterFactory);
    }

    public Layer2NewDescriptor? GenerateNewDescriptor(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        return null;
    }

    public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx,
        Action<IILGen> pushDescriptor, Type targetType)
    {
        pushCtx(ilGenerator);
        pushReader(ilGenerator);
        ilGenerator.Callvirt(
            typeof(ITypeBinaryDeserializerContext).GetMethod(nameof(ITypeBinaryDeserializerContext.LoadEncryptedString))
            !);
        if (targetType != typeof(object))
        {
            if (targetType != GetPreferredType())
                throw new ArgumentOutOfRangeException(nameof(targetType));
            return;
        }

        ilGenerator.Box(GetPreferredType());
    }

    public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        pushCtx(ilGenerator);
        pushReader(ilGenerator);
        ilGenerator.Callvirt(
            typeof(ITypeBinaryDeserializerContext).GetMethod(nameof(ITypeBinaryDeserializerContext.SkipEncryptedString))
            !);
    }

    public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx,
        Action<IILGen> pushValue, Type valueType)
    {
        pushCtx(ilGenerator);
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Callvirt(
            typeof(ITypeBinarySerializerContext).GetMethod(nameof(ITypeBinarySerializerContext.StoreEncryptedString))!);
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
