using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using BTDB.EventStoreLayer;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public static class BasicSerializersFactory
    {
        static BasicSerializersFactory()
        {
            var fh = new List<IFieldHandler>();
            var des = new List<ITypeDescriptor>();
            AddJustOrderable(fh, "StringOrderable",
                             EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadStringOrdered()),
                             EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipStringOrdered()),
                             EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteStringOrdered(null)));
            Add(fh, des, "String",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadString()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipString()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteString(null)));
            Add(fh, des, "UInt8",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadUInt8()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipUInt8()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteUInt8(0)));
            AddJustOrderable(fh, "Int8Orderable",
                             EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadInt8Ordered()),
                             EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipInt8Ordered()),
                             EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteInt8Ordered(0)));
            Add(fh, des, "Int8",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadInt8()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipInt8()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteInt8(0)));
            fh.Add(new SignedFieldHandler());
            fh.Add(new UnsignedFieldHandler());
            AddDescriptor(des, "VInt16",
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVInt16()),
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVInt16()),
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVInt16(0)));
            AddDescriptor(des, "VUInt16",
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVUInt16()),
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVUInt16()),
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVUInt16(0)));
            AddDescriptor(des, "VInt32",
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVInt32()),
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVInt32()),
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVInt32(0)));
            AddDescriptor(des, "VUInt32",
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVUInt32()),
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVUInt32()),
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVUInt32(0)));
            AddDescriptor(des, "VInt64",
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVInt64()),
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVInt64()),
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVInt64(0)));
            AddDescriptor(des, "VUInt64",
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVUInt64()),
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVUInt64()),
                          EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVUInt64(0)));
            Add(fh, des, "Bool",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadBool()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipBool()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteBool(false)));
            Add(fh, des, "Single",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadSingle()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipSingle()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteSingle(0)));
            Add(fh, des, "Double",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadDouble()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipDouble()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteDouble(0)));
            Add(fh, des, "Decimal",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadDecimal()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipDecimal()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteDecimal(0)));
            AddJustOrderable(fh, "DateTime",
                             EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadDateTime()),
                             EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipDateTime()),
                             EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteDateTimeForbidUnspecifiedKind(new DateTime())));
            Add(fh, des, "DateTime",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadDateTime()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipDateTime()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteDateTime(new DateTime())));
            Add(fh, des, "TimeSpan",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadTimeSpan()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipTimeSpan()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteTimeSpan(new TimeSpan())));
            Add(fh, des, "Guid",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadGuid()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipGuid()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteGuid(new Guid())));
            fh.Add(new ByteArrayLastFieldHandler());
            fh.Add(new ByteArrayFieldHandler());
            des.Add(new ByteArrayTypeDescriptor());
            Add(fh, des, "IPAddress",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadIPAddress()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipIPAddress()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteIPAddress(IPAddress.Any)));
            Add(fh, des, "Version",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadVersion()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipVersion()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteVersion(null)));
            fh.Add(new OrderedEncryptedStringHandler());
            fh.Add(new EncryptedStringHandler());
            des.Add(new EncryptedStringDescriptor());
            Add(fh, des, "DateTimeOffset",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadDateTimeOffset()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipDateTimeOffset()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteDateTimeOffset(new DateTimeOffset())));
            Add(fh, des, "StringValues",
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).ReadStringValues()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedReader).SkipStringValues()),
                EmitHelpers.GetMethodInfo(() => default(AbstractBufferedWriter).WriteStringValues(new Microsoft.Extensions.Primitives.StringValues())));
            FieldHandlers = fh.ToArray();
            TypeDescriptors = des.ToArray();
        }

        static void AddJustOrderable(ICollection<IFieldHandler> fh, string name, MethodInfo readMethodInfo, MethodInfo skipMethodInfo, MethodInfo writeMethodInfo)
        {
            fh.Add(new SimpleFieldHandlerJustOrderableBase(name, readMethodInfo, skipMethodInfo, writeMethodInfo));
        }

        static void AddDescriptor(ICollection<ITypeDescriptor> des, string name, MethodInfo readMethodInfo, MethodInfo skipMethodInfo, MethodInfo writeMethodInfo)
        {
            des.Add(new SimpleTypeDescriptor(name, readMethodInfo, skipMethodInfo, writeMethodInfo));
        }

        static void Add(ICollection<IFieldHandler> fh, ICollection<ITypeDescriptor> des, string name, MethodInfo readMethodInfo, MethodInfo skipMethodInfo, MethodInfo writeMethodInfo)
        {
            fh.Add(new SimpleFieldHandlerBase(name, readMethodInfo, skipMethodInfo, writeMethodInfo));
            des.Add(new SimpleTypeDescriptor(name, readMethodInfo, skipMethodInfo, writeMethodInfo));
        }

        public static readonly IFieldHandler[] FieldHandlers;
        public static readonly ITypeDescriptor[] TypeDescriptors;
    }
}
