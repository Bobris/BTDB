using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public class EnumFieldHandler : IFieldHandler
    {
        readonly byte[] _configuration;
        readonly bool _signed;
        Type _enumType;

        public class EnumConfiguration
        {
            readonly bool _signed;
            readonly bool _flags;
            readonly string[] _names;
            readonly ulong[] _values;

            public EnumConfiguration(Type enumType)
            {
                _signed = IsSignedEnum(enumType);
                _flags = IsFlagsEnum(enumType);
                _names = enumType.GetEnumNames();
                var undertype = enumType.GetEnumUnderlyingType();
                var enumValues = enumType.GetEnumValues();
                IEnumerable<ulong> enumValuesUlongs;
                if (undertype == typeof(int)) enumValuesUlongs = enumValues.Cast<int>().Select(i => (ulong)i);
                else if (undertype == typeof(uint)) enumValuesUlongs = enumValues.Cast<uint>().Select(i => (ulong)i);
                else if (undertype == typeof(sbyte)) enumValuesUlongs = enumValues.Cast<sbyte>().Select(i => (ulong)i);
                else if (undertype == typeof(byte)) enumValuesUlongs = enumValues.Cast<byte>().Select(i => (ulong)i);
                else if (undertype == typeof(short)) enumValuesUlongs = enumValues.Cast<short>().Select(i => (ulong)i);
                else if (undertype == typeof(ushort)) enumValuesUlongs = enumValues.Cast<ushort>().Select(i => (ulong)i);
                else if (undertype == typeof(long)) enumValuesUlongs = enumValues.Cast<long>().Select(i => (ulong)i);
                else enumValuesUlongs = enumValues.Cast<ulong>();
                _values = enumValuesUlongs.ToArray();
            }

            public EnumConfiguration(byte[] configuration)
            {
                var reader = new ByteArrayReader(configuration);
                var header = reader.ReadVUInt32();
                _signed = (header & 1) != 0;
                _flags = (header & 2) != 0;
                var count = header >> 2;
                _names = new string[count];
                _values = new ulong[count];
                for (int i = 0; i < count; i++) Names[i] = reader.ReadString();
                if (_signed)
                {
                    for (int i = 0; i < count; i++) Values[i] = (ulong)reader.ReadVInt64();
                }
                else
                {
                    for (int i = 0; i < count; i++) Values[i] = reader.ReadVUInt64();
                }
            }

            public bool Signed
            {
                get
                {
                    return _signed;
                }
            }

            bool Flags
            {
                get { return _flags; }
            }

            string[] Names
            {
                get { return _names; }
            }

            ulong[] Values
            {
                get { return _values; }
            }

            public byte[] ToConfiguration()
            {
                var writer = new ByteArrayWriter();
                writer.WriteVUInt32((_signed ? 1u : 0) + (Flags ? 2u : 0) + 4u * (uint)Names.Length);
                foreach (var name in Names)
                {
                    writer.WriteString(name);
                }
                foreach (var value in Values)
                {
                    if (_signed) writer.WriteVInt64((long)value);
                    else writer.WriteVUInt64(value);
                }
                return writer.Data;
            }

            public Type ToType()
            {
                var name = string.Format("Enum{0}", Guid.NewGuid());
                AssemblyBuilder ab = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(name), AssemblyBuilderAccess.RunAndCollect);
                ModuleBuilder mb = ab.DefineDynamicModule(name, true);
                var enumBuilder = mb.DefineEnum(name, TypeAttributes.Public, _signed ? typeof(long) : typeof(ulong));
                for (int i = 0; i < Names.Length; i++)
                {
                    if (_signed)
                    {
                        enumBuilder.DefineLiteral(Names[i], (long)Values[i]);
                    }
                    else
                    {
                        enumBuilder.DefineLiteral(Names[i], Values[i]);
                    }
                }
                return enumBuilder.CreateType();
            }

            public static bool operator == (EnumConfiguration left,EnumConfiguration right)
            {
                if (left.Flags != right.Flags) return false;
                if (!left.Names.SequenceEqual(right.Names)) return false;
                if (!left.Values.SequenceEqual(right.Values)) return false;
                return true;
            }

            public static bool operator !=(EnumConfiguration left, EnumConfiguration right)
            {
                return !(left == right);
            }
        }

        public EnumFieldHandler(Type enumType)
        {
            if (!IsCompatibleWith(enumType)) throw new ArgumentException("enumType");
            _enumType = enumType;
            var ec = new EnumConfiguration(enumType);
            _signed = ec.Signed;
            _configuration = ec.ToConfiguration();
        }

        public EnumFieldHandler(byte[] configuration)
        {
            _configuration = configuration;
            var ec = new EnumConfiguration(configuration);
            _signed = ec.Signed;
        }

        static bool IsSignedEnum(Type enumType)
        {
            return SignedFieldHandler.IsCompatibleWith(enumType.GetEnumUnderlyingType());
        }

        static bool IsFlagsEnum(Type type)
        {
            return type.GetCustomAttributes(typeof(FlagsAttribute), false).Length != 0;
        }

        public static string HandlerName
        {
            get { return "Enum"; }
        }

        public string Name
        {
            get { return HandlerName; }
        }

        public byte[] Configuration
        {
            get { return _configuration; }
        }

        public static bool IsCompatibleWith(Type type)
        {
            if (!type.IsEnum) return false;
            var enumUnderlyingType = type.GetEnumUnderlyingType();
            return SignedFieldHandler.IsCompatibleWith(enumUnderlyingType) || UnsignedFieldHandler.IsCompatibleWith(enumUnderlyingType);
        }

        public Type HandledType()
        {
            return _enumType ?? (_enumType = new EnumConfiguration(_configuration).ToType());
        }

        public bool NeedsCtx()
        {
            return false;
        }

        public void Load(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            Type typeRead;
            if (_signed)
            {
                ilGenerator.Call(() => ((AbstractBufferedReader)null).ReadVInt64());
                typeRead = typeof(long);
            }
            else
            {
                ilGenerator.Call(() => ((AbstractBufferedReader)null).ReadVUInt64());
                typeRead = typeof(ulong);
            }
            new DefaultTypeConvertorGenerator().GenerateConversion(typeRead, _enumType.GetEnumUnderlyingType())(ilGenerator);
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            if (_signed)
            {
                ilGenerator.Call(() => ((AbstractBufferedReader)null).SkipVInt64());
            }
            else
            {
                ilGenerator.Call(() => ((AbstractBufferedReader)null).SkipVUInt64());
            }
        }

        public void Save(ILGenerator ilGenerator, Action<ILGenerator> pushWriterOrCtx, Action<ILGenerator> pushValue)
        {
            pushWriterOrCtx(ilGenerator);
            pushValue(ilGenerator);
            if (_signed)
            {
                ilGenerator
                    .ConvI8()
                    .Call(() => ((AbstractBufferedWriter)null).WriteVInt64(0));
            }
            else
            {
                ilGenerator
                    .ConvU8()
                    .Call(() => ((AbstractBufferedWriter)null).WriteVUInt64(0));
            }
        }

        bool IFieldHandler.IsCompatibleWith(Type type)
        {
            return IsCompatibleWith(type);
        }

        public void InformAboutDestinationHandler(IFieldHandler dstHandler)
        {
            if (_enumType != null) return;
            if ((dstHandler is EnumFieldHandler) == false) return;
            if (dstHandler.Configuration.SequenceEqual(Configuration))
            {
                _enumType = dstHandler.HandledType();
            }
        }
    }
}