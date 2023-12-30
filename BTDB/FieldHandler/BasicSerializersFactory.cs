using System.Collections.Generic;
using System.Reflection;
using BTDB.EventStoreLayer;
using BTDB.StreamLayer;

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
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteStringOrdered))!);
        Add(fh, des, "String",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadString))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipString))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteString))!);
        Add(fh, des, "UInt8",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadUInt8))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip1Byte))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteUInt8))!);
        AddJustOrderable(fh, "Int8Orderable",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadInt8Ordered))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip1Byte))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteInt8Ordered))!);
        Add(fh, des, "Int8",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadInt8))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip1Byte))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteInt8))!);
        fh.Add(new SignedFieldHandler());
        fh.Add(new UnsignedFieldHandler());
        AddDescriptor(des, "VInt16",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVInt16))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVInt16))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVInt16))!);
        AddDescriptor(des, "VUInt16",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt16))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVUInt16))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVUInt16))!);
        AddDescriptor(des, "VInt32",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVInt32))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVInt32))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVInt32))!);
        AddDescriptor(des, "VUInt32",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt32))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVUInt32))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVUInt32))!);
        AddDescriptor(des, "VInt64",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVInt64))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVInt64))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVInt64))!);
        AddDescriptor(des, "VUInt64",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt64))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVUInt64))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVUInt64))!);
        Add(fh, des, "Bool",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadBool))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip1Byte))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteBool))!);
        fh.Add(new ForbidOrderableFloatsFieldHandler());
        Add(fh, des, "Single",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadSingle))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip4Bytes))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteSingle))!);
        Add(fh, des, "Double",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadDouble))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip8Bytes))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteDouble))!);
        Add(fh, des, "Decimal",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadDecimal))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipDecimal))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteDecimal))!);
        AddJustOrderable(fh, "DateTime",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadDateTime))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip8Bytes))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteDateTimeForbidUnspecifiedKind))!);
        Add(fh, des, "DateTime",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadDateTime))!,
            typeof(MemReader).GetMethod(nameof(MemReader.Skip8Bytes))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteDateTime))!);
        Add(fh, des, "TimeSpan",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadTimeSpan))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVInt64))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteTimeSpan))!);
        Add(fh, des, "Guid",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadGuid))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipGuid))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteGuid))!);
        fh.Add(new ByteArrayLastFieldHandler());
        fh.Add(new ByteArrayFieldHandler());
        des.Add(new ByteArrayTypeDescriptor());
        Add(fh, des, "IPAddress",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadIPAddress))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipIPAddress))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteIPAddress))!);
        Add(fh, des, "Version",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadVersion))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipVersion))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVersion))!);
        fh.Add(new OrderedEncryptedStringHandler());
        fh.Add(new EncryptedStringHandler());
        des.Add(new EncryptedStringDescriptor());
        Add(fh, des, "DateTimeOffset",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadDateTimeOffset))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipDateTimeOffset))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteDateTimeOffset))!);
        Add(fh, des, "StringValues",
            typeof(MemReader).GetMethod(nameof(MemReader.ReadStringValues))!,
            typeof(MemReader).GetMethod(nameof(MemReader.SkipStringValues))!,
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteStringValues))!);
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
