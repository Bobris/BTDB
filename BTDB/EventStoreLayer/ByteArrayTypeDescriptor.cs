using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.Serialization;
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

    public bool Equals(ITypeDescriptor other, Dictionary<ITypeDescriptor, ITypeDescriptor>? equalities)
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

    public IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields =>
        Array.Empty<KeyValuePair<string, ITypeDescriptor>>();

    public IEnumerable<Type> GetNativeTypes()
    {
        yield return typeof(ByteBuffer);
        yield return typeof(ReadOnlyMemory<byte>);
    }

    public bool AnyOpNeedsCtx()
    {
        return false;
    }

    public Layer2Loader GenerateLoad(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        if (targetType == typeof(byte[]))
        {
            return static (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, byte[]>(ref value) = reader.ReadByteArray();
            };
        }

        if (targetType == typeof(ByteBuffer))
        {
            return static (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, ByteBuffer>(ref value) = ByteBuffer.NewAsync(reader.ReadByteArray()!);
            };
        }

        if (targetType == typeof(ReadOnlyMemory<byte>))
        {
            return static (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, ReadOnlyMemory<byte>>(ref value) = reader.ReadByteArrayAsMemory();
            };
        }

        return this.BuildConvertingLoader(typeof(byte[]), targetType, typeConverterFactory);
    }

    public void Skip(ref MemReader reader, ITypeBinaryDeserializerContext? ctx)
    {
        reader.SkipByteArray();
    }

    public Layer2Saver GenerateSave(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        if (targetType == typeof(byte[]))
        {
            return static (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteByteArray(Unsafe.As<byte, byte[]>(ref value));
            };
        }

        if (targetType == typeof(ByteBuffer))
        {
            return static (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteByteArray(Unsafe.As<byte, ByteBuffer>(ref value));
            };
        }

        if (targetType == typeof(ReadOnlyMemory<byte>))
        {
            return static (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteByteArray(Unsafe.As<byte, ReadOnlyMemory<byte>>(ref value));
            };
        }

        return this.BuildConvertingSaver(typeof(byte[]), targetType, typeConverterFactory);
    }

    public Layer2NewDescriptor? GenerateNewDescriptor(Type targetType, ITypeConverterFactory typeConverterFactory,
        bool forbidSerializationOfLazyDBObjects)
    {
        return null;
    }

    public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx,
        Action<IILGen> pushDescriptor, Type targetType)
    {
        pushReader(ilGenerator);
        if (targetType == typeof(ReadOnlyMemory<byte>))
        {
            ilGenerator.Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadByteArrayAsMemory))!);
            return;
        }

        ilGenerator.Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadByteArray))!);
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
        ilGenerator.Call(typeof(MemReader).GetMethod(nameof(MemReader.SkipByteArray))!);
    }

    public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx,
        Action<IILGen> pushValue, Type valueType)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        if (valueType == typeof(byte[]))
            ilGenerator.Call(
                typeof(MemWriter).GetMethod(nameof(MemWriter.WriteByteArray), new[] { typeof(byte[]) })!);
        else if (valueType == typeof(ByteBuffer))
            ilGenerator.Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteByteArray),
                new[] { valueType })!);
        else if (valueType == typeof(ReadOnlyMemory<byte>))
            ilGenerator.Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteByteArray),
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
