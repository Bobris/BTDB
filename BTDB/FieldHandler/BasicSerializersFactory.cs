using System;
using System.Collections.Generic;
using System.Reflection;
using BTDB.EventStoreLayer;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public static class BasicSerializersFactory
{
    static BasicSerializersFactory()
    {
        var fh = new List<IFieldHandler>();
        var des = new List<ITypeDescriptor>();
        AddJustOrderable(fh, "StringOrderable",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadStringOrdered))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipStringOrdered))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteStringOrdered))!);
        Add(fh, des, "String",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadString))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipString))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteString))!);
        Add(fh, des, "UInt8",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadUInt8))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipUInt8))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteUInt8))!);
        AddJustOrderable(fh, "Int8Orderable",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadInt8Ordered))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipInt8Ordered))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteInt8Ordered))!);
        Add(fh, des, "Int8",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadInt8))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipInt8))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteInt8))!);
        fh.Add(new SignedFieldHandler());
        fh.Add(new UnsignedFieldHandler());
        AddDescriptor(des, "VInt16",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVInt16))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVInt16))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteVInt16))!);
        AddDescriptor(des, "VUInt16",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVUInt16))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVUInt16))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteVUInt16))!);
        AddDescriptor(des, "VInt32",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVInt32))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVInt32))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteVInt32))!);
        AddDescriptor(des, "VUInt32",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVUInt32))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVUInt32))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteVUInt32))!);
        AddDescriptor(des, "VInt64",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVInt64))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVInt64))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteVInt64))!);
        AddDescriptor(des, "VUInt64",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVUInt64))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVUInt64))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteVUInt64))!);
        Add(fh, des, "Bool",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadBool))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipBool))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteBool))!);
        Add(fh, des, "Single",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadSingle))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipSingle))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteSingle))!);
        Add(fh, des, "Double",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadDouble))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipDouble))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteDouble))!);
        Add(fh, des, "Decimal",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadDecimal))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipDecimal))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteDecimal))!);
        AddJustOrderable(fh, "DateTime",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadDateTime))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipDateTime))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteDateTimeForbidUnspecifiedKind))!);
        Add(fh, des, "DateTime",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadDateTime))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipDateTime))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteDateTime))!);
        Add(fh, des, "TimeSpan",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadTimeSpan))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipTimeSpan))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteTimeSpan))!);
        Add(fh, des, "Guid",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadGuid))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipGuid))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteGuid))!);
        fh.Add(new ByteArrayLastFieldHandler());
        fh.Add(new ByteArrayFieldHandler());
        des.Add(new ByteArrayTypeDescriptor());
        Add(fh, des, "IPAddress",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadIPAddress))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipIPAddress))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteIPAddress))!);
        Add(fh, des, "Version",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVersion))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVersion))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteVersion))!);
        fh.Add(new OrderedEncryptedStringHandler());
        fh.Add(new EncryptedStringHandler());
        des.Add(new EncryptedStringDescriptor());
        Add(fh, des, "DateTimeOffset",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadDateTimeOffset))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipDateTimeOffset))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteDateTimeOffset))!);
        Add(fh, des, "StringValues",
            typeof(SpanReader).GetMethod(nameof(SpanReader.ReadStringValues))!,
            typeof(SpanReader).GetMethod(nameof(SpanReader.SkipStringValues))!,
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteStringValues))!);
        FieldHandlers = fh.ToArray();
        TypeDescriptors = des.ToArray();
    }

    static void AddJustOrderable(ICollection<IFieldHandler> fh, string name, MethodInfo readMethodInfo,
        MethodInfo skipMethodInfo, MethodInfo writeMethodInfo)
    {
        fh.Add(new SimpleFieldHandlerJustOrderableBase(name, readMethodInfo, skipMethodInfo, writeMethodInfo));
    }

    static void AddDescriptor(ICollection<ITypeDescriptor> des, string name, MethodInfo readMethodInfo,
        MethodInfo skipMethodInfo, MethodInfo writeMethodInfo)
    {
        des.Add(new SimpleTypeDescriptor(name, readMethodInfo, skipMethodInfo, writeMethodInfo));
    }

    static void Add(ICollection<IFieldHandler> fh, ICollection<ITypeDescriptor> des, string name,
        MethodInfo readMethodInfo, MethodInfo skipMethodInfo, MethodInfo writeMethodInfo)
    {
        fh.Add(new SimpleFieldHandlerBase(name, readMethodInfo, skipMethodInfo, writeMethodInfo));
        des.Add(new SimpleTypeDescriptor(name, readMethodInfo, skipMethodInfo, writeMethodInfo));
    }

    public static readonly IFieldHandler[] FieldHandlers;
    public static readonly ITypeDescriptor[] TypeDescriptors;
}
