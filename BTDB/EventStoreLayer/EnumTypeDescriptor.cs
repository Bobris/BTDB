using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    internal class EnumTypeDescriptor : ITypeDescriptor, IPersistTypeDescriptor, ITypeBinarySerializerGenerator, ITypeBinarySkipperGenerator
    {
        Type _type;
        readonly string _name;
        readonly bool _signed;
        readonly bool _flags;
        readonly List<KeyValuePair<string, ulong>> _pairs;

        public EnumTypeDescriptor(TypeSerializers typeSerializers, Type type)
        {
            _type = type;
            _name = typeSerializers.TypeToName(type);
            _signed = IsSignedEnum(type);
            _flags = IsFlagsEnum(type);
            var undertype = type.GetEnumUnderlyingType();
            var enumValues = type.GetEnumValues();
            IEnumerable<ulong> enumValuesUlongs;
            if (undertype == typeof(int)) enumValuesUlongs = enumValues.Cast<int>().Select(i => (ulong)i);
            else if (undertype == typeof(uint)) enumValuesUlongs = enumValues.Cast<uint>().Select(i => (ulong)i);
            else if (undertype == typeof(sbyte)) enumValuesUlongs = enumValues.Cast<sbyte>().Select(i => (ulong)i);
            else if (undertype == typeof(byte)) enumValuesUlongs = enumValues.Cast<byte>().Select(i => (ulong)i);
            else if (undertype == typeof(short)) enumValuesUlongs = enumValues.Cast<short>().Select(i => (ulong)i);
            else if (undertype == typeof(ushort)) enumValuesUlongs = enumValues.Cast<ushort>().Select(i => (ulong)i);
            else if (undertype == typeof(long)) enumValuesUlongs = enumValues.Cast<long>().Select(i => (ulong)i);
            else enumValuesUlongs = enumValues.Cast<ulong>();
            _pairs = type.GetEnumNames().Zip(enumValuesUlongs.ToArray(), (s, v) => new KeyValuePair<string, ulong>(s, v)).ToList();
        }

        public EnumTypeDescriptor(AbstractBufferedReader reader)
        {
            _name = reader.ReadString();
            var header = reader.ReadVUInt32();
            _signed = (header & 1) != 0;
            _flags = (header & 2) != 0;
            var count = header >> 2;
            _pairs = new List<KeyValuePair<string, ulong>>((int)count);
            for (int i = 0; i < count; i++)
            {
                _pairs.Add(_signed
                               ? new KeyValuePair<string, ulong>(reader.ReadString(), (ulong) reader.ReadVInt64())
                               : new KeyValuePair<string, ulong>(reader.ReadString(), reader.ReadVUInt64()));
            }
        }

        static bool IsSignedEnum(Type enumType)
        {
            return SignedFieldHandler.IsCompatibleWith(enumType.GetEnumUnderlyingType());
        }

        static bool IsFlagsEnum(Type type)
        {
            return type.GetCustomAttributes(typeof(FlagsAttribute), false).Length != 0;
        }

        public bool Equals(ITypeDescriptor other)
        {
            return Equals(other, new HashSet<ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance));
        }

        public string Name
        {
            get { return _name; }
        }

        public void FinishBuildFromType(ITypeDescriptorFactory factory)
        {
        }

        public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
        {
            if (stack.Contains(this))
            {
                text.Append(Name);
                return;
            }
            stack.Add(this);
            text.AppendLine(Name);
            AppendIndent(text, indent);
            text.Append("enum ");
            if (_flags) text.Append("flags ");
            text.AppendLine("{");
            foreach (var pair in _pairs)
            {
                AppendIndent(text, indent + 1);
                text.Append(pair.Key);
                text.Append(" = ");
                if (_signed) text.Append((long)pair.Value); else text.Append(pair.Value);
                text.AppendLine();
            }
            AppendIndent(text, indent);
            text.Append("}");
        }

        static void AppendIndent(StringBuilder text, uint indent)
        {
            text.Append(' ', (int)(indent * 4));
        }

        public bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack)
        {
            var o = other as EnumTypeDescriptor;
            if (o == null) return false;
            if (Name != o.Name) return false;
            if (_flags != o._flags) return false;
            if (_signed != o._signed) return false;
            return _pairs.SequenceEqual(o._pairs);
        }

        public Type GetPreferedType()
        {
            return _type;
        }

        public ITypeBinaryDeserializerGenerator BuildBinaryDeserializerGenerator(Type target)
        {
            return new Deserializer(this, target);
        }

        class Deserializer : ITypeBinaryDeserializerGenerator
        {
            readonly EnumTypeDescriptor _owner;
            readonly Type _target;

            public Deserializer(EnumTypeDescriptor owner, Type target)
            {
                _owner = owner;
                _target = target;
            }

            public bool LoadNeedsCtx()
            {
                return false;
            }

            public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
            {
                pushReader(ilGenerator);
                Type typeRead;
                if (_owner._signed)
                {
                    ilGenerator.Call(() => default(AbstractBufferedReader).ReadVInt64());
                    typeRead = typeof(long);
                }
                else
                {
                    ilGenerator.Call(() => default(AbstractBufferedReader).ReadVUInt64());
                    typeRead = typeof(ulong);
                }
                new DefaultTypeConvertorGenerator().GenerateConversion(typeRead, _target.GetEnumUnderlyingType())(ilGenerator);
            }
        }

        public ITypeBinarySkipperGenerator BuildBinarySkipperGenerator()
        {
            return this;
        }

        public ITypeBinarySerializerGenerator BuildBinarySerializerGenerator()
        {
            return this;
        }

        public ITypeNewDescriptorGenerator BuildNewDescriptorGenerator()
        {
            return null;
        }

        public IEnumerable<ITypeDescriptor> NestedTypes()
        {
            yield break;
        }

        public void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
        {
        }

        public bool Sealed { get { return true; } }
        public bool StoredInline { get { return true; } }

        public void ClearMappingToType()
        {
            _type = null;
        }

        public void Persist(AbstractBufferedWriter writer, Action<AbstractBufferedWriter, ITypeDescriptor> nestedDescriptorPersistor)
        {
            writer.WriteString(_name);
            writer.WriteVUInt32((_signed ? 1u : 0) + (_flags ? 2u : 0) + 4u * (uint)_pairs.Count);
            foreach (var pair in _pairs)
            {
                writer.WriteString(pair.Key);
                if (_signed)
                    writer.WriteVInt64((long) pair.Value);
                else
                    writer.WriteVUInt64(pair.Value);
            }
        }

        public bool SaveNeedsCtx()
        {
            return false;
        }

        public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue)
        {
            pushWriter(ilGenerator);
            pushValue(ilGenerator);
            if (_signed)
            {
                ilGenerator
                    .ConvI8()
                    .Call(() => default(AbstractBufferedWriter).WriteVInt64(0));
            }
            else
            {
                ilGenerator
                    .ConvU8()
                    .Call(() => default(AbstractBufferedWriter).WriteVUInt64(0));
            }
        }

        public bool SkipNeedsCtx()
        {
            return false;
        }

        public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
        {
            pushReader(ilGenerator);
            if (_signed)
            {
                ilGenerator.Call(() => default(AbstractBufferedReader).SkipVInt64());
            }
            else
            {
                ilGenerator.Call(() => default(AbstractBufferedReader).SkipVUInt64());
            }
        }
    }
}