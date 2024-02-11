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
    CallGetterAndWriteInt8,
    GetByOffsetAndWriteInt8,
    CallGetterAndWriteInt16,
    GetByOffsetAndWriteInt16,
    CallGetterAndWriteInt32,
    GetByOffsetAndWriteInt32,
    CallGetterAndWriteInt64,
    GetByOffsetAndWriteInt64,
    CallGetterAndWriteUInt8,
    GetByOffsetAndWriteUInt8,
    CallGetterAndWriteUInt16,
    GetByOffsetAndWriteUInt16,
    CallGetterAndWriteUInt32,
    GetByOffsetAndWriteUInt32,
    CallGetterAndWriteUInt64,
    GetByOffsetAndWriteUInt64,
    CallGetterAndWriteFloat16,
    GetByOffsetAndWriteFloat16,
    CallGetterAndWriteFloat32,
    GetByOffsetAndWriteFloat32,
    CallGetterAndWriteFloat64,
    GetByOffsetAndWriteFloat64,
    CallGetterAndWriteBool,
    GetByOffsetAndWriteBool,
    CallGetterAndWriteDateTime,
    GetByOffsetAndWriteDateTime,
    CallGetterAndWriteGuid,
    GetByOffsetAndWriteGuid
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
        _memWriter = new();
    }

    public void AddField(FieldMetadata field)
    {
        _memWriter.WriteUInt8((byte)BonSerializerCmd.WriteKey);
        _memWriter.WriteStringInUtf8(field.Name);
        if (field.Type == typeof(string))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteString,
                BonSerializerCmd.GetByOffsetAndWriteString);
        }
        else if (field.Type == typeof(bool))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteBool, BonSerializerCmd.GetByOffsetAndWriteBool);
        }
        else if (field.Type == typeof(DateTime))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteDateTime,
                BonSerializerCmd.GetByOffsetAndWriteDateTime);
        }
        else if (field.Type == typeof(Guid))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteGuid, BonSerializerCmd.GetByOffsetAndWriteGuid);
        }
        else if (field.Type == typeof(sbyte))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteInt8, BonSerializerCmd.GetByOffsetAndWriteInt8);
        }
        else if (field.Type == typeof(short))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteInt16,
                BonSerializerCmd.GetByOffsetAndWriteInt16);
        }
        else if (field.Type == typeof(int))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteInt32,
                BonSerializerCmd.GetByOffsetAndWriteInt32);
        }
        else if (field.Type == typeof(long))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteInt64,
                BonSerializerCmd.GetByOffsetAndWriteInt64);
        }
        else if (field.Type == typeof(byte))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteUInt8,
                BonSerializerCmd.GetByOffsetAndWriteUInt8);
        }
        else if (field.Type == typeof(ushort) || field.Type == typeof(char))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteUInt16,
                BonSerializerCmd.GetByOffsetAndWriteUInt16);
        }
        else if (field.Type == typeof(uint))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteUInt32,
                BonSerializerCmd.GetByOffsetAndWriteUInt32);
        }
        else if (field.Type == typeof(ulong))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteUInt64,
                BonSerializerCmd.GetByOffsetAndWriteUInt64);
        }
        else if (field.Type == typeof(Half))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteFloat16,
                BonSerializerCmd.GetByOffsetAndWriteFloat16);
        }
        else if (field.Type == typeof(float))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteFloat32,
                BonSerializerCmd.GetByOffsetAndWriteFloat32);
        }
        else if (field.Type == typeof(double))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteFloat64,
                BonSerializerCmd.GetByOffsetAndWriteFloat64);
        }
        else if (!field.Type.IsValueType)
        {
        }
        else
        {
            throw new InvalidOperationException("Unsupported type " + field.Type);
        }
    }

    unsafe void AddFieldWithCmds(FieldMetadata field, BonSerializerCmd callGetter, BonSerializerCmd getByOffset)
    {
        if (field.PropRefGetter != null)
        {
            _memWriter.WriteUInt8((byte)callGetter);
            _memWriter.WritePointer((nint)field.PropRefGetter);
        }
        else
        {
            _memWriter.WriteUInt8((byte)getByOffset);
            _memWriter.WriteVUInt32(field.ByteOffset!.Value);
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
                    {
                        var len = reader.ReadVUInt32();
                        ctx.Builder.WriteKey(reader.ReadBlockAsSpan(len));
                        break;
                    }
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
                    case BonSerializerCmd.CallGetterAndWriteBool:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, bool>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteBool:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, bool>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteDateTime:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, DateTime>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteDateTime:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, DateTime>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteGuid:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, Guid>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteGuid:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, Guid>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteInt8:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, sbyte>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteInt8:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, sbyte>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteInt16:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, short>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteInt16:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, short>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
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
                    case BonSerializerCmd.CallGetterAndWriteInt64:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, long>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteInt64:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, long>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteUInt8:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteUInt8:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, byte>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteUInt16:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, ushort>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteUInt16:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, ushort>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteUInt32:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, uint>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteUInt32:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, uint>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteUInt64:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, ulong>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteUInt64:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, ulong>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteFloat16:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write((double)Unsafe.As<UInt128, Half>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteFloat16:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write((double)
                            Unsafe.As<byte, Half>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteFloat32:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, float>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteFloat32:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, float>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteFloat64:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref ctx.TempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, double>(ref ctx.TempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteFloat64:
                    {
                        var offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, double>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
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
