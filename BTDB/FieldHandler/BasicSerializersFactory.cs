using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using BTDB.EventStoreLayer;
using BTDB.StreamLayer;
using Microsoft.Extensions.Primitives;

namespace BTDB.FieldHandler;

public static class BasicSerializersFactory
{
    static BasicSerializersFactory()
    {
        var fh = new List<IFieldHandler>();
        var des = new List<ITypeDescriptor>();
        AddJustOrderable(fh, "StringOrderable",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadStringOrdered))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipStringOrdered))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteStringOrdered))!,
            (ref MemReader reader, IReaderCtx? _) => reader.SkipStringOrdered(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, string>(ref value) = reader.ReadStringOrdered();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteStringOrdered(Unsafe.As<byte, string>(ref value));
            }
        );
        Add(fh, des, "String",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadString))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipString))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteString))!,
            (ref MemReader reader, IReaderCtx? _) => reader.SkipString(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, string>(ref value) = reader.ReadString();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteString(Unsafe.As<byte, string>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, string>(ref value) = reader.ReadString();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipString(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteString(Unsafe.As<byte, string>(ref value));
            });
        Add(fh, des, "UInt8",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadUInt8))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip1Byte))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteUInt8))!,
            (ref MemReader reader, IReaderCtx? _) => reader.Skip1Byte(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, byte>(ref value) = reader.ReadUInt8();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteUInt8(Unsafe.As<byte, byte>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, byte>(ref value) = reader.ReadUInt8();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.Skip1Byte(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteUInt8(Unsafe.As<byte, byte>(ref value));
            });
        AddJustOrderable(fh, "Int8Orderable",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadInt8Ordered))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip1Byte))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteInt8Ordered))!,
            (ref MemReader reader, IReaderCtx? _) => reader.Skip1Byte()
            , (ref MemReader reader, IReaderCtx? _, ref byte value) => { Unsafe.As<byte, sbyte>(ref value) = reader.ReadInt8Ordered(); },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteInt8Ordered(Unsafe.As<byte, sbyte>(ref value));
            });
        Add(fh, des, "Int8",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadInt8))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip1Byte))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteInt8))!,
            (ref MemReader reader, IReaderCtx? _) => reader.Skip1Byte(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, sbyte>(ref value) = reader.ReadInt8();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteInt8(Unsafe.As<byte, sbyte>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, sbyte>(ref value) = reader.ReadInt8();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.Skip1Byte(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteInt8(Unsafe.As<byte, sbyte>(ref value));
            });
        fh.Add(new SignedFieldHandler());
        fh.Add(new UnsignedFieldHandler());
        AddDescriptor(des, "VInt16",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVInt16))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVInt16))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVInt16))!,
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, short>(ref value) = reader.ReadVInt16();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipVInt16(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteVInt16(Unsafe.As<byte, short>(ref value));
            });
        AddDescriptor(des, "VUInt16",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt16))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVUInt16))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVUInt16))!,
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, ushort>(ref value) = reader.ReadVUInt16();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipVUInt16(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteVUInt16(Unsafe.As<byte, ushort>(ref value));
            });
        AddDescriptor(des, "VInt32",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVInt32))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVInt32))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVInt32))!,
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, int>(ref value) = reader.ReadVInt32();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipVInt32(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteVInt32(Unsafe.As<byte, int>(ref value));
            });
        AddDescriptor(des, "VUInt32",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt32))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVUInt32))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVUInt32))!,
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, uint>(ref value) = reader.ReadVUInt32();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipVUInt32(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteVUInt32(Unsafe.As<byte, uint>(ref value));
            });
        AddDescriptor(des, "VInt64",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVInt64))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVInt64))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVInt64))!,
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, long>(ref value) = reader.ReadVInt64();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipVInt64(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteVInt64(Unsafe.As<byte, long>(ref value));
            });
        AddDescriptor(des, "VUInt64",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt64))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVUInt64))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVUInt64))!,
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, ulong>(ref value) = reader.ReadVUInt64();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipVUInt64(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteVUInt64(Unsafe.As<byte, ulong>(ref value));
            });
        Add(fh, des, "Bool",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadBool))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip1Byte))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteBool))!,
            (ref MemReader reader, IReaderCtx? _) => reader.Skip1Byte(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, bool>(ref value) = reader.ReadBool();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteBool(Unsafe.As<byte, bool>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, bool>(ref value) = reader.ReadBool();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.Skip1Byte(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteBool(Unsafe.As<byte, bool>(ref value));
            });
        fh.Add(new ForbidOrderableFloatsFieldHandler());
        Add(fh, des, "Single",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadSingle))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip4Bytes))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteSingle))!,
            (ref MemReader reader, IReaderCtx? _) => reader.Skip4Bytes(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, float>(ref value) = reader.ReadSingle();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteSingle(Unsafe.As<byte, float>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, float>(ref value) = reader.ReadSingle();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.Skip4Bytes(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteSingle(Unsafe.As<byte, float>(ref value));
            });
        Add(fh, des, "Double",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadDouble))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip8Bytes))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteDouble))!,
            (ref MemReader reader, IReaderCtx? _) => reader.Skip8Bytes(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, double>(ref value) = reader.ReadDouble();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteDouble(Unsafe.As<byte, double>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, double>(ref value) = reader.ReadDouble();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.Skip8Bytes(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteDouble(Unsafe.As<byte, double>(ref value));
            });
        Add(fh, des, "Decimal",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadDecimal))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipDecimal))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteDecimal))!,
            (ref MemReader reader, IReaderCtx? _) => reader.SkipDecimal(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, decimal>(ref value) = reader.ReadDecimal();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteDecimal(Unsafe.As<byte, decimal>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, decimal>(ref value) = reader.ReadDecimal();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipDecimal(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteDecimal(Unsafe.As<byte, decimal>(ref value));
            });
        AddJustOrderable(fh, "DateTime",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadDateTime))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip8Bytes))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteDateTimeForbidUnspecifiedKind))!,
            (ref MemReader reader, IReaderCtx? _) => reader.Skip8Bytes(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, DateTime>(ref value) = reader.ReadDateTime();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteDateTimeForbidUnspecifiedKind(Unsafe.As<byte, DateTime>(ref value));
            });
        Add(fh, des, "DateTime",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadDateTime))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip8Bytes))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteDateTime))!,
            (ref MemReader reader, IReaderCtx? _) => reader.Skip8Bytes(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, DateTime>(ref value) = reader.ReadDateTime();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteDateTime(Unsafe.As<byte, DateTime>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, DateTime>(ref value) = reader.ReadDateTime();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.Skip8Bytes(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteDateTime(Unsafe.As<byte, DateTime>(ref value));
            });
        Add(fh, des, "TimeSpan",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadTimeSpan))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVInt64))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteTimeSpan))!,
            (ref MemReader reader, IReaderCtx? _) => reader.SkipVInt64(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, TimeSpan>(ref value) = reader.ReadTimeSpan();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteTimeSpan(Unsafe.As<byte, TimeSpan>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, TimeSpan>(ref value) = reader.ReadTimeSpan();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipVInt64(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteTimeSpan(Unsafe.As<byte, TimeSpan>(ref value));
            });
        Add(fh, des, "Guid",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadGuid))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipGuid))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteGuid))!,
            (ref MemReader reader, IReaderCtx? _) => reader.SkipGuid(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, Guid>(ref value) = reader.ReadGuid();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteGuid(Unsafe.As<byte, Guid>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, Guid>(ref value) = reader.ReadGuid();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipGuid(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteGuid(Unsafe.As<byte, Guid>(ref value));
            });
        fh.Add(new ByteArrayLastFieldHandler());
        fh.Add(new ByteArrayFieldHandler());
        des.Add(new ByteArrayTypeDescriptor());
        Add(fh, des, "IPAddress",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadIPAddress))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipIPAddress))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteIPAddress))!,
            (ref MemReader reader, IReaderCtx? _) => reader.SkipIPAddress(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, System.Net.IPAddress>(ref value) = reader.ReadIPAddress();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteIPAddress(Unsafe.As<byte, System.Net.IPAddress>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, System.Net.IPAddress>(ref value) = reader.ReadIPAddress();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipIPAddress(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteIPAddress(Unsafe.As<byte, System.Net.IPAddress>(ref value));
            });
        Add(fh, des, "Version",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVersion))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVersion))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVersion))!,
            (ref MemReader reader, IReaderCtx? _) => reader.SkipVersion(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, Version>(ref value) = reader.ReadVersion();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteVersion(Unsafe.As<byte, Version>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, Version>(ref value) = reader.ReadVersion();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipVersion(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteVersion(Unsafe.As<byte, Version>(ref value));
            });
        fh.Add(new OrderedEncryptedStringHandler());
        fh.Add(new EncryptedStringHandler());
        des.Add(new EncryptedStringDescriptor());
        Add(fh, des, "DateTimeOffset",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadDateTimeOffset))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipDateTimeOffset))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteDateTimeOffset))!,
            (ref MemReader reader, IReaderCtx? _) => reader.SkipDateTimeOffset(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, DateTimeOffset>(ref value) = reader.ReadDateTimeOffset();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteDateTimeOffset(Unsafe.As<byte, DateTimeOffset>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, DateTimeOffset>(ref value) = reader.ReadDateTimeOffset();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipDateTimeOffset(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteDateTimeOffset(Unsafe.As<byte, DateTimeOffset>(ref value));
            });
        Add(fh, des, "StringValues",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadStringValues))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipStringValues))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteStringValues))!,
            (ref MemReader reader, IReaderCtx? _) => reader.SkipStringValues(),
            (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, StringValues>(ref value) = reader.ReadStringValues();
            },
            (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteStringValues(Unsafe.As<byte, StringValues>(ref value));
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _, ref byte value) =>
            {
                Unsafe.As<byte, StringValues>(ref value) = reader.ReadStringValues();
            },
            (ref MemReader reader, ITypeBinaryDeserializerContext? _) => reader.SkipStringValues(),
            (ref MemWriter writer, ITypeBinarySerializerContext? _, ref byte value) =>
            {
                writer.WriteStringValues(Unsafe.As<byte, StringValues>(ref value));
            });
        FieldHandlers = fh.ToArray();
        TypeDescriptors = des.ToArray();
    }

    static void AddJustOrderable(ICollection<IFieldHandler> fh, string name, MethodInfo readMethodInfo,
        MethodInfo skipMethodInfo, MethodInfo writeMethodInfo, SkipReaderCtxFunc skipReader,
        FieldHandlerLoad loaderReader, FieldHandlerSave saverWriter)
    {
        fh.Add(new SimpleFieldHandlerJustOrderableBase(name, readMethodInfo, skipMethodInfo, writeMethodInfo,
            skipReader, loaderReader, saverWriter));
    }

    static void AddDescriptor(ICollection<ITypeDescriptor> des, string name, MethodInfo readMethodInfo,
        MethodInfo skipMethodInfo, MethodInfo writeMethodInfo, Layer2Loader load, Layer2Skipper skip, Layer2Saver save)
    {
        des.Add(new SimpleTypeDescriptor(name, readMethodInfo, skipMethodInfo, writeMethodInfo, load, skip, save));
    }

    static void Add(ICollection<IFieldHandler> fh, ICollection<ITypeDescriptor> des, string name,
        MethodInfo readMethodInfo, MethodInfo skipMethodInfo, MethodInfo writeMethodInfo, SkipReaderCtxFunc skipReader,
        FieldHandlerLoad loaderReader, FieldHandlerSave saverWriter, Layer2Loader load, Layer2Skipper skip,
        Layer2Saver save)
    {
        fh.Add(new SimpleFieldHandlerBase(name, readMethodInfo, skipMethodInfo, writeMethodInfo, skipReader,
            loaderReader, saverWriter));
        des.Add(new SimpleTypeDescriptor(name, readMethodInfo, skipMethodInfo, writeMethodInfo, load, skip, save));
    }

    public static readonly IFieldHandler[] FieldHandlers;
    public static readonly ITypeDescriptor[] TypeDescriptors;
}
