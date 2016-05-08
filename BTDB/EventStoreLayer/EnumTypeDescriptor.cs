using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    class EnumTypeDescriptor : ITypeDescriptor, IPersistTypeDescriptor
    {
        readonly ITypeDescriptorCallbacks _typeSerializers;
        Type _type;
        readonly string _name;
        readonly bool _signed;
        readonly bool _flags;
        readonly List<KeyValuePair<string, ulong>> _pairs;

        public EnumTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, Type type)
        {
            _typeSerializers = typeSerializers;
            _type = type;
            _name = typeSerializers.TypeNameMapper.ToName(type);
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

        public EnumTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, AbstractBufferedReader reader)
        {
            _typeSerializers = typeSerializers;
            _name = reader.ReadString();
            var header = reader.ReadVUInt32();
            _signed = (header & 1) != 0;
            _flags = (header & 2) != 0;
            var count = header >> 2;
            _pairs = new List<KeyValuePair<string, ulong>>((int)count);
            for (int i = 0; i < count; i++)
            {
                _pairs.Add(_signed
                               ? new KeyValuePair<string, ulong>(reader.ReadString(), (ulong)reader.ReadVInt64())
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

        public string Name => _name;

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

        public class DynamicEnum : IKnowDescriptor
        {
            readonly ITypeDescriptor _descriptor;
            readonly ulong _value;

            public DynamicEnum(long value, ITypeDescriptor descriptor)
            {
                _value = (ulong)value;
                _descriptor = descriptor;
            }

            public DynamicEnum(ulong value, ITypeDescriptor descriptor)
            {
                _value = value;
                _descriptor = descriptor;
            }

            public ITypeDescriptor GetDescriptor()
            {
                return _descriptor;
            }

            public override string ToString()
            {
                return ((EnumTypeDescriptor)_descriptor).UlongValueToString(_value);
            }

            public override int GetHashCode()
            {
                return _value.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                if (obj == null) return false;
                var objMe = obj as DynamicEnum;
                if (objMe != null)
                {
                    if (objMe._descriptor != _descriptor) return false;
                    return objMe._value == _value;
                }
                if (!obj.GetType().IsEnum) return false;
                var myDescriptor = ((EnumTypeDescriptor)_descriptor);
                var otherDescriptor = myDescriptor._typeSerializers.DescriptorOf(obj.GetType());
                if (!myDescriptor.Equals(otherDescriptor)) return false;
                if (myDescriptor._signed)
                {
                    return _value == (ulong)Convert.ToInt64(obj, CultureInfo.InvariantCulture);
                }
                return _value == Convert.ToUInt64(obj, CultureInfo.InvariantCulture);
            }
        }

        string UlongValueToString(ulong value)
        {
            if (_flags)
            {
                return UlongValueToStringFlags(value);
            }
            var index = _pairs.FindIndex(p => p.Value == value);
            if (index < 0) return UlongValueToStringAsNumber(value);
            return _pairs[index].Key;
        }

        string UlongValueToStringAsNumber(ulong value)
        {
            if (_signed)
            {
                return ((long)value).ToString(CultureInfo.InvariantCulture);
            }
            return value.ToString(CultureInfo.InvariantCulture);
        }

        string UlongValueToStringFlags(ulong value)
        {
            var workingValue = value;
            var index = _pairs.Count - 1;
            var stringBuilder = new StringBuilder();
            var isFirstText = true;
            while (index >= 0)
            {
                var currentValue = _pairs[index].Value;
                if ((index == 0) && (currentValue == 0L))
                {
                    break;
                }
                if ((workingValue & currentValue) == currentValue)
                {
                    workingValue -= currentValue;
                    if (!isFirstText)
                    {
                        stringBuilder.Insert(0, ", ");
                    }
                    stringBuilder.Insert(0, _pairs[index].Key);
                    isFirstText = false;
                }
                index--;
            }
            if (workingValue != 0L)
            {
                return UlongValueToStringAsNumber(value);
            }
            if (value != 0)
            {
                return stringBuilder.ToString();
            }
            if ((_pairs.Count > 0) && (_pairs[0].Value == 0))
            {
                return _pairs[0].Key;
            }
            return "0";
        }

        public bool AnyOpNeedsCtx()
        {
            return false;
        }

        public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type targetType)
        {
            pushReader(ilGenerator);
            Type typeRead;
            if (_signed)
            {
                ilGenerator.Call(() => default(AbstractBufferedReader).ReadVInt64());
                typeRead = typeof(long);
            }
            else
            {
                ilGenerator.Call(() => default(AbstractBufferedReader).ReadVUInt64());
                typeRead = typeof(ulong);
            }
            if (targetType == typeof(object))
            {
                ilGenerator.Do(pushDescriptor);
                if (_signed)
                {
                    ilGenerator.Newobj(() => new DynamicEnum(0L, null));
                }
                else
                {
                    ilGenerator.Newobj(() => new DynamicEnum(0UL, null));
                }
                ilGenerator.Castclass(typeof(object));
                return;
            }
            new DefaultTypeConvertorGenerator().GenerateConversion(typeRead, targetType.GetEnumUnderlyingType())(ilGenerator);
        }

        public ITypeNewDescriptorGenerator BuildNewDescriptorGenerator()
        {
            return null;
        }

        public ITypeDescriptor NestedType(int index)
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
            _type = null;
        }

        public bool ContainsField(string name)
        {
            return false;
        }

        public void Persist(AbstractBufferedWriter writer, Action<AbstractBufferedWriter, ITypeDescriptor> nestedDescriptorPersistor)
        {
            writer.WriteString(_name);
            writer.WriteVUInt32((_signed ? 1u : 0) + (_flags ? 2u : 0) + 4u * (uint)_pairs.Count);
            foreach (var pair in _pairs)
            {
                writer.WriteString(pair.Key);
                if (_signed)
                    writer.WriteVInt64((long)pair.Value);
                else
                    writer.WriteVUInt64(pair.Value);
            }
        }

        public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue, Type valueType)
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