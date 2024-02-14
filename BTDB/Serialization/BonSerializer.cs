using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Bon;
using BTDB.IL;
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
    GetCustomAndWriteString,
    CallGetterAndWriteInt8,
    GetByOffsetAndWriteInt8,
    GetCustomAndWriteInt8,
    CallGetterAndWriteInt16,
    GetByOffsetAndWriteInt16,
    GetCustomAndWriteInt16,
    CallGetterAndWriteInt32,
    GetByOffsetAndWriteInt32,
    GetCustomAndWriteInt32,
    CallGetterAndWriteInt64,
    GetByOffsetAndWriteInt64,
    GetCustomAndWriteInt64,
    CallGetterAndWriteUInt8,
    GetByOffsetAndWriteUInt8,
    GetCustomAndWriteUInt8,
    CallGetterAndWriteUInt16,
    GetByOffsetAndWriteUInt16,
    GetCustomAndWriteUInt16,
    CallGetterAndWriteUInt32,
    GetByOffsetAndWriteUInt32,
    GetCustomAndWriteUInt32,
    CallGetterAndWriteUInt64,
    GetByOffsetAndWriteUInt64,
    GetCustomAndWriteUInt64,
    CallGetterAndWriteFloat16,
    GetByOffsetAndWriteFloat16,
    GetCustomAndWriteFloat16,
    CallGetterAndWriteFloat32,
    GetByOffsetAndWriteFloat32,
    GetCustomAndWriteFloat32,
    CallGetterAndWriteFloat64,
    GetByOffsetAndWriteFloat64,
    GetCustomAndWriteFloat64,
    CallGetterAndWriteBool,
    GetByOffsetAndWriteBool,
    GetCustomAndWriteBool,
    CallGetterAndWriteDateTime,
    GetByOffsetAndWriteDateTime,
    GetCustomAndWriteDateTime,
    CallGetterAndWriteGuid,
    GetByOffsetAndWriteGuid,
    GetCustomAndWriteGuid,
    CallGetterAndWriteObject,
    GetByOffsetAndWriteObject,
    GetCustomAndWriteObject,
    InitArray,
    NextArray,
    InitList,
    InitHashSet,
    NextHashSet
}

public ref struct BonSerializerCtx
{
    public ref BonBuilder Builder;
}

public delegate void BonSerialize(ref BonSerializerCtx ctx, ref byte value);

public class BonSerializerFactory
{
    readonly Type _type;
    MemWriter _memWriter;

    static readonly ConcurrentDictionary<nint, BonSerialize> Cache = new();

    public static void Serialize(ref BonBuilder builder, object? value)
    {
        if (value == null)
        {
            builder.WriteNull();
            return;
        }

        var type = value.GetType();
        var serializer = Create(type);
        var ctx = new BonSerializerCtx { Builder = ref builder };
        serializer(ref ctx, ref Unsafe.As<object, byte>(ref value));
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

    public BonSerializerFactory(Type type)
    {
        _type = type;
        _memWriter = new();
    }

    public void AddField(FieldMetadata field)
    {
        _memWriter.WriteUInt8((byte)BonSerializerCmd.WriteKey);
        _memWriter.WriteStringInUtf8(field.Name);
        var type = field.Type;
        WriteCmdByType(field, type);
    }

    void WriteCmdByType(FieldMetadata? field, Type type)
    {
        if (type == typeof(string))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteString);
        }
        else if (type == typeof(bool))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteBool);
        }
        else if (type == typeof(DateTime))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteDateTime);
        }
        else if (type == typeof(Guid))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteGuid);
        }
        else if (type == typeof(sbyte))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteInt8);
        }
        else if (type == typeof(short))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteInt16);
        }
        else if (type == typeof(int))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteInt32);
        }
        else if (type == typeof(long))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteInt64);
        }
        else if (type == typeof(byte))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteUInt8);
        }
        else if (type == typeof(ushort) || type == typeof(char))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteUInt16);
        }
        else if (type == typeof(uint))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteUInt32);
        }
        else if (type == typeof(ulong))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteUInt64);
        }
        else if (type == typeof(Half))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteFloat16);
        }
        else if (type == typeof(float))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteFloat32);
        }
        else if (type == typeof(double))
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteFloat64);
        }
        else if (type.IsEnum)
        {
            WriteCmdByType(field, Enum.GetUnderlyingType(type));
        }
        else if (!type.IsValueType)
        {
            AddFieldWithCmds(field, BonSerializerCmd.CallGetterAndWriteObject);
        }
        else
        {
            throw new InvalidOperationException("Unsupported type " + type);
        }
    }

    unsafe void AddFieldWithCmds(FieldMetadata? field, BonSerializerCmd callGetter)
    {
        if (field == null)
        {
            _memWriter.WriteUInt8((byte)(callGetter + 2));
        }
        else if (field.PropRefGetter != null)
        {
            _memWriter.WriteUInt8((byte)callGetter);
            _memWriter.WritePointer((nint)field.PropRefGetter);
        }
        else
        {
            _memWriter.WriteUInt8((byte)(callGetter + 1));
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
        if (_type.IsArray)
        {
            if (!_type.IsSZArray) throw new InvalidOperationException("Only SZArray is supported");
            _memWriter.WriteUInt8((byte)BonSerializerCmd.InitArray);
            var elementType = _type.GetElementType()!;
            WriteCmdByType(null, elementType);
            _memWriter.WriteUInt8((byte)BonSerializerCmd.NextArray);
            return;
        }

        if (_type.SpecializationOf(typeof(List<>)) is { } listType)
        {
            _memWriter.WriteUInt8((byte)BonSerializerCmd.InitList);
            var elementType = listType.GetGenericArguments()[0];
            WriteCmdByType(null, elementType);
            _memWriter.WriteUInt8((byte)BonSerializerCmd.NextArray);
            return;
        }

        if (_type.SpecializationOf(typeof(HashSet<>)) is { } hashSetType)
        {
            _memWriter.WriteUInt8((byte)BonSerializerCmd.InitHashSet);
            var elementType = hashSetType.GetGenericArguments()[0];
            var layout = RawData.GetHashSetEntriesLayout(elementType);
            _memWriter.WriteVUInt32(layout.Offset);
            WriteCmdByType(null, elementType);
            _memWriter.WriteUInt8((byte)BonSerializerCmd.NextHashSet);
            return;
        }

        var classMetadata = ReflectionMetadata.FindByType(_type);
        if (classMetadata != null)
        {
            AddClass(classMetadata);
            return;
        }

        throw new NotSupportedException("BonSerialization of " + _type.ToSimpleName() + " is not supported.");
    }

    public unsafe BonSerialize Build()
    {
        _memWriter.WriteUInt8((byte)BonSerializerCmd.Return);
        var memory = _memWriter.GetPersistentMemoryAndReset();
        return (ref BonSerializerCtx ctx, ref byte value) =>
        {
            using var memoryHandle = memory.Pin();
            var reader = new MemReader(memoryHandle.Pointer, memory.Length);
            object? tempObject = null;
            object? tempObject2 = null;
            uint offset = 0;
            uint offsetDelta = 0;
            uint offsetOffset = 0;
            uint offsetFinal = 0;
            uint offsetLabel = 0;
            UInt128 tempBytes = default;
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
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<object, byte>(ref tempObject));
                        ctx.Builder.Write(Unsafe.As<object, string>(ref tempObject));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteString:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, string>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteString:
                    {
                        ctx.Builder.Write(
                            Unsafe.As<byte, string>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteObject:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<object, byte>(ref tempObject));
                        Serialize(ref ctx.Builder, tempObject);
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteObject:
                    {
                        offset = reader.ReadVUInt32();
                        Serialize(ref ctx.Builder,
                            Unsafe.As<byte, object>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteObject:
                    {
                        Serialize(ref ctx.Builder, Unsafe.As<byte, object>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteBool:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, bool>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteBool:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, bool>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteBool:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, bool>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteDateTime:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, DateTime>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteDateTime:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, DateTime>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteDateTime:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, DateTime>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteGuid:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, Guid>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteGuid:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, Guid>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteGuid:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, Guid>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteInt8:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, sbyte>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteInt8:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, sbyte>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteInt8:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, sbyte>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteInt16:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, short>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteInt16:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, short>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteInt16:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, short>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteInt32:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, int>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteInt32:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, int>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteInt32:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, int>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteInt64:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, long>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteInt64:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, long>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteInt64:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, long>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteUInt8:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, byte>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteUInt8:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, byte>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteUInt8:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, byte>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteUInt16:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, ushort>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteUInt16:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, ushort>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteUInt16:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, ushort>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteUInt32:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, uint>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteUInt32:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, uint>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteUInt32:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, uint>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteUInt64:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, ulong>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteUInt64:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, ulong>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteUInt64:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, ulong>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteFloat16:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write((double)Unsafe.As<UInt128, Half>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteFloat16:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write((double)
                            Unsafe.As<byte, Half>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteFloat16:
                    {
                        ctx.Builder.Write((double)Unsafe.As<byte, Half>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteFloat32:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, float>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteFloat32:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, float>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteFloat32:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, float>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.CallGetterAndWriteFloat64:
                    {
                        var getter = (delegate*<object, ref byte, void>)reader.ReadPointer();
                        getter(Unsafe.As<byte, object>(ref value), ref Unsafe.As<UInt128, byte>(ref tempBytes));
                        ctx.Builder.Write(Unsafe.As<UInt128, double>(ref tempBytes));
                        break;
                    }
                    case BonSerializerCmd.GetByOffsetAndWriteFloat64:
                    {
                        offset = reader.ReadVUInt32();
                        ctx.Builder.Write(
                            Unsafe.As<byte, double>(ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset)));
                        break;
                    }
                    case BonSerializerCmd.GetCustomAndWriteFloat64:
                    {
                        ctx.Builder.Write(Unsafe.As<byte, double>(ref RawData.Ref(tempObject2, offset)));
                        break;
                    }
                    case BonSerializerCmd.InitArray:
                    {
                        tempObject2 = Unsafe.As<byte, object>(ref value);
                        var count = Unsafe.As<byte, int>(ref RawData.Ref(tempObject2, (uint)Unsafe.SizeOf<nint>()));
                        ref readonly var mt = ref RawData.MethodTableRef(tempObject2);
                        offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                        offsetDelta = mt.ComponentSize;
                        offsetFinal = offset + offsetDelta * (uint)count;
                        offsetLabel = (uint)reader.GetCurrentPositionWithoutController();
                        ctx.Builder.StartArray();
                        break;
                    }
                    case BonSerializerCmd.NextArray:
                    {
                        offset += offsetDelta;
                        if (offset < offsetFinal)
                        {
                            reader.SetCurrentPositionWithoutController(offsetLabel);
                        }
                        else
                        {
                            ctx.Builder.FinishArray();
                            return;
                        }

                        break;
                    }
                    case BonSerializerCmd.InitList:
                    {
                        tempObject2 = Unsafe.As<byte, object>(ref value);
                        var count = Unsafe.As<ICollection>(tempObject2).Count;
                        tempObject2 = RawData.ListItems(Unsafe.As<List<object>>(tempObject2));
                        ref readonly var mt = ref RawData.MethodTableRef(tempObject2);
                        offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                        offsetDelta = mt.ComponentSize;
                        offsetFinal = offset + offsetDelta * (uint)count;
                        offsetLabel = (uint)reader.GetCurrentPositionWithoutController();
                        ctx.Builder.StartArray();
                        break;
                    }
                    case BonSerializerCmd.InitHashSet:
                    {
                        tempObject2 = Unsafe.As<byte, object>(ref value);
                        var count = Unsafe.As<byte, int>(ref RawData.Ref(tempObject2,
                            RawData.Align(8 + 4 * (uint)Unsafe.SizeOf<nint>(), 8)));
                        tempObject2 = RawData.HashSetEntries(Unsafe.As<HashSet<object>>(tempObject2));
                        offsetOffset = reader.ReadVUInt32();
                        ref readonly var mt = ref RawData.MethodTableRef(tempObject2);
                        offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                        offsetDelta = mt.ComponentSize;
                        offsetFinal = offset + offsetDelta * (uint)count;
                        offsetLabel = (uint)reader.GetCurrentPositionWithoutController();
                        ctx.Builder.StartArray();
                        while (offset < offsetFinal)
                        {
                            if (Unsafe.As<byte, int>(ref RawData.Ref(tempObject2, offset + 4)) >= -1) break;
                            offset += offsetDelta;
                        }

                        if (offset >= offsetFinal)
                        {
                            ctx.Builder.FinishArray();
                            return;
                        }

                        offset += offsetOffset;
                        break;
                    }
                    case BonSerializerCmd.NextHashSet:
                    {
                        offset += offsetDelta - offsetOffset;
                        while (offset < offsetFinal)
                        {
                            if (Unsafe.As<byte, int>(ref RawData.Ref(tempObject2, offset + 4)) >= -1) break;
                            offset += offsetDelta;
                        }

                        if (offset < offsetFinal)
                        {
                            reader.SetCurrentPositionWithoutController(offsetLabel);
                            offset += offsetOffset;
                        }
                        else
                        {
                            ctx.Builder.FinishArray();
                            return;
                        }

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
