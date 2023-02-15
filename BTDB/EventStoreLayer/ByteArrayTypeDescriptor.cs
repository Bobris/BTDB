using System;
using System.Collections.Generic;
using System.Text;
using BTDB.Buffer;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

public class ByteArrayTypeDescriptor : ITypeDescriptorMultipleNativeTypes
{
    public string Name => "Byte[]";

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
        return typeof(byte[]);
    }

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

    public bool LoadNeedsHelpWithConversion => false;

    public void ClearMappingToType()
    {
    }

    public bool ContainsField(string name)
    {
        return false;
    }

    public IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields => Array.Empty<KeyValuePair<string, ITypeDescriptor>>();

    public IEnumerable<Type> GetNativeTypes()
    {
        yield return typeof(ByteBuffer);
        yield return typeof(ReadOnlyMemory<byte>);
    }

    public bool AnyOpNeedsCtx()
    {
        return false;
    }

    public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx,
        Action<IILGen> pushDescriptor, Type targetType)
    {
        pushReader(ilGenerator);
        if (targetType == typeof(ReadOnlyMemory<byte>))
        {
            ilGenerator.Call(typeof(SpanReader).GetMethod(nameof(SpanReader.ReadByteArrayAsMemory))!);
            return;
        }
        ilGenerator.Call(typeof(SpanReader).GetMethod(nameof(SpanReader.ReadByteArray))!);
        if (targetType == typeof(ByteBuffer))
        {
            ilGenerator.Call(() => ByteBuffer.NewAsync(null));
            return;
        }


        if (targetType != typeof(object))
        {
            if (targetType != typeof(byte[]))
                throw new ArgumentOutOfRangeException(nameof(targetType));
            return;
        }

        ilGenerator.Castclass(typeof(object));
    }

    public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(typeof(SpanReader).GetMethod(nameof(SpanReader.SkipByteArray))!);
    }

    public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx,
        Action<IILGen> pushValue, Type valueType)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        if (valueType == typeof(byte[]))
            ilGenerator.Call(
                typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteByteArray), new[] { typeof(byte[]) })!);
        else if (valueType == typeof(ByteBuffer))
            ilGenerator.Call(typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteByteArray),
                new[] { valueType })!);
        else if (valueType == typeof(ReadOnlyMemory<byte>))
            ilGenerator.Call(typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteByteArray),
                new[] { valueType })!);
        else throw new ArgumentOutOfRangeException(nameof(valueType));
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
