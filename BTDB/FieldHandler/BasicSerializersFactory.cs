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
                             EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadStringOrdered()),
                             EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipStringOrdered()),
                             EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteStringOrdered(null)));
            Add(fh, des, "String",
                EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadString()),
                EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipString()),
                EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteString(null)));
            Add(fh, des, "UInt8",
                EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadUInt8()),
                EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipUInt8()),
                EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteUInt8(0)));
            AddJustOrderable(fh, "Int8Orderable",
                             EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadInt8Ordered()),
                             EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipInt8Ordered()),
                             EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteInt8Ordered(0)));
            Add(fh, des, "Int8",
                EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadInt8()),
                EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipInt8()),
                EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteInt8(0)));
            fh.Add(new SignedFieldHandler());
            fh.Add(new UnsignedFieldHandler());
            AddDescriptor(des, "VInt16",
                          EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadVInt16()),
                          EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipVInt16()),
                          EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteVInt16(0)));
            AddDescriptor(des, "VUInt16",
                          EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadVUInt16()),
                          EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipVUInt16()),
                          EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteVUInt16(0)));
            AddDescriptor(des, "VInt32",
                          EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadVInt32()),
                          EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipVInt32()),
                          EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteVInt32(0)));
            AddDescriptor(des, "VUInt32",
                          EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadVUInt32()),
                          EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipVUInt32()),
                          EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteVUInt32(0)));
            AddDescriptor(des, "VInt64",
                          EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadVInt64()),
                          EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipVInt64()),
                          EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteVInt64(0)));
            AddDescriptor(des, "VUInt64",
                          EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadVUInt64()),
                          EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipVUInt64()),
                          EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteVUInt64(0)));
            Add(fh, des, "Bool",
                EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadBool()),
                EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipBool()),
                EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteBool(false)));
            Add(fh, des, "Single",
                EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadSingle()),
                EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipSingle()),
                EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteSingle(0)));
            Add(fh, des, "Double",
                EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadDouble()),
                EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipDouble()),
                EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteDouble(0)));
            Add(fh, des, "Decimal",
                EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadDecimal()),
                EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipDecimal()),
                EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteDecimal(0)));
            AddJustOrderable(fh, "DateTime",
                             EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadDateTime()),
                             EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipDateTime()),
                             EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteDateTimeForbidUnspecifiedKind(new DateTime())));
            Add(fh, des, "DateTime",
                EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadDateTime()),
                EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipDateTime()),
                EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteDateTime(new DateTime())));
            Add(fh, des, "TimeSpan",
                EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadTimeSpan()),
                EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipTimeSpan()),
                EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteTimeSpan(new TimeSpan())));
            Add(fh, des, "Guid",
                EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadGuid()),
                EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipGuid()),
                EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteGuid(new Guid())));
            fh.Add(new ByteArrayLastFieldHandler());
            fh.Add(new ByteArrayFieldHandler());
            des.Add(new ByteArrayTypeDescriptor());
            Add(fh, des, "IPAddress",
                EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadIPAddress()),
                EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipIPAddress()),
                EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteIPAddress(IPAddress.Any)));
            Add(fh, des, "Version",
                EmitHelpers.GetMethodInfo(() => default(SpanReader).ReadVersion()),
                EmitHelpers.GetMethodInfo(() => default(SpanReader).SkipVersion()),
                EmitHelpers.GetMethodInfo(() => default(SpanWriter).WriteVersion(null)));
            fh.Add(new OrderedEncryptedStringHandler());
            fh.Add(new EncryptedStringHandler());
            des.Add(new EncryptedStringDescriptor());
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
