using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.CompilerServices;
using BTDB.Bon;
using BTDB.StreamLayer;

namespace BTDB.Serialization;

enum BonSerializerCmd : byte
{
    Return = 0,
    StartClass,
    FinishClass,
    WriteKey,
    CallGetterAndWriteString,
    GetByOffsetAndWriteString,
    CallGetterAndWriteInt32,
    GetByOffsetAndWriteInt32
}

public ref struct BonSerializerCtx
{
    public ref BonBuilder Builder;
    public object TempObject;
    public UInt128 TempBytes;
}

public delegate void BonSerialize(ref BonSerializerCtx ctx, ref byte value);

public class BonSerializerFactory
{
    readonly Type _type;
    MemWriter _memWriter;

    static readonly ConcurrentDictionary<nint, BonSerialize> Cache = new();

    public static void Serialize(ref BonBuilder builder, object value)
    {
        var type = value.GetType();
        var serializer = Create(type);
        var ctx = new BonSerializerCtx { Builder = ref builder };
        ref var valueRef = ref Unsafe.As<object, byte>(ref value);
        serializer(ref ctx, ref valueRef);
    }

    public static BonSerialize Create(Type type)
    {
        if (Cache.TryGetValue(type.TypeHandle.Value, out var res))
            return res;
        var factory = new BonSerializerFactory(type);
        factory.Generate();
        res = factory.Build();
        Cache.TryAdd(type.TypeHandle.Value, res);
        Cache.TryGetValue(type.TypeHandle.Value, out res);
        return res!;
    }

    public static void AnyObjectSerializer(ref BonSerializerCtx ctx, ref byte value)
    {
        var type = Unsafe.As<byte, object>(ref value).GetType();
        var serializer = Create(type);
        serializer(ref ctx, ref value);
    }

    public BonSerializerFactory(Type type)
    {
        _type = type;
        _memWriter = new MemWriter();
    }

    public unsafe void AddField(FieldMetadata field)
    {
        _memWriter.WriteUInt8((byte)BonSerializerCmd.WriteKey);
        _memWriter.WriteStringInUtf8(field.Name);
        if (field.Type == typeof(string))
        {
            if (field.PropRefGetter != null)
            {
                _memWriter.WriteUInt8((byte)BonSerializerCmd.CallGetterAndWriteString);
                _memWriter.WritePointer((nint)field.PropRefGetter);
            }
            else
            {
                _memWriter.WriteUInt8((byte)BonSerializerCmd.GetByOffsetAndWriteString);
                _memWriter.WriteVUInt32(field.ByteOffset!.Value);
            }
        }
        else if (field.Type == typeof(int))
        {
            if (field.PropRefGetter != null)
            {
                _memWriter.WriteUInt8((byte)BonSerializerCmd.CallGetterAndWriteInt32);
                _memWriter.WritePointer((nint)field.PropRefGetter);
            }
            else
            {
                _memWriter.WriteUInt8((byte)BonSerializerCmd.GetByOffsetAndWriteInt32);
                _memWriter.WriteVUInt32(field.ByteOffset!.Value);
            }
        }
        else if (!field.Type.IsValueType)
        {
        }
        else
        {
            throw new InvalidOperationException("Unsupported type " + field.Type);
        }
    }

    public void AddClass(ClassMetadata classMetadata)
    {
        var persistName = classMetadata.PersistedName ?? (string.IsNullOrEmpty(classMetadata.Namespace)
            ? classMetadata.Name
            : classMetadata.Namespace + "." + classMetadata.Name);
        _memWriter.WriteUInt8((byte)BonSerializerCmd.StartClass);
        _memWriter.WriteStringInUtf8(persistName);
        foreach (var fieldMetadata in classMetadata.Fields)
        {
            AddField(fieldMetadata);
        }

        _memWriter.WriteUInt8((byte)BonSerializerCmd.FinishClass);
    }

    public void Generate()
    {
        var classMetadata = ReflectionMetadata.FindByType(_type);
        if (classMetadata != null)
        {
            AddClass(classMetadata);
            return;
        }
    }

    public unsafe BonSerialize Build()
    {
        _memWriter.WriteUInt8((byte)BonSerializerCmd.Return);
        var memory = _memWriter.GetPersistentMemoryAndReset();
        return (ref BonSerializerCtx ctx, ref byte value) =>
        {
            using var memoryHandle = memory.Pin();
            var reader = new MemReader(memoryHandle.Pointer, memory.Length);
            while (true)
            {
                var cmd = (BonSerializerCmd)reader.ReadUInt8();
                switch (cmd)
                {
                    case BonSerializerCmd.Return:
                        return;
                    case BonSerializerCmd.StartClass:
                        ctx.Builder.StartClass(reader.ReadStringInUtf8());
                        break;
                    case BonSerializerCmd.FinishClass:
                        ctx.Builder.FinishClass();
                        break;
                    case BonSerializerCmd.WriteKey:
                        ctx.Builder.WriteKey(reader.ReadStringInUtf8());
                        break;
                    case BonSerializerCmd.CallGetterAndWriteString:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<object, byte>(ref ctx.TempObject));
                        ctx.Builder.Write(Unsafe.As<object, string>(ref ctx.TempObject));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteString:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, string>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteInt32:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, int>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteInt32:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, int>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    default:
                        throw new InvalidDataException("Unknown command in BonSerializer " + (byte)cmd + " at " +
                                                       (reader.GetCurrentPosition() - 1));
                }
            }
        };
    }
}
