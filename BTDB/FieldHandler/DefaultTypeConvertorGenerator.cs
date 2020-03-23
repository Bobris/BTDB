using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.IL;

namespace BTDB.FieldHandler
{
    public class DefaultTypeConvertorGenerator : ITypeConvertorGenerator
    {
        readonly Dictionary<Tuple<Type, Type>, Action<IILGen>> _conversions = new Dictionary<Tuple<Type, Type>, Action<IILGen>>();

        public static ITypeConvertorGenerator Instance = new DefaultTypeConvertorGenerator();

        public DefaultTypeConvertorGenerator()
        {
            var convConvertibleTypes = new[]
                                           {
                                               typeof (byte), typeof (sbyte), typeof (ushort), typeof (short),
                                               typeof (uint), typeof (int), typeof (ulong), typeof (long),
                                               typeof (float), typeof (double)
                                           };
            AddConversions(convConvertibleTypes, typeof(long), ilg => ilg.ConvI8());
            AddConversions(convConvertibleTypes, typeof(ulong), ilg => ilg.ConvU8());
            AddConversions(convConvertibleTypes, typeof(int), ilg => ilg.ConvI4());
            AddConversions(convConvertibleTypes, typeof(uint), ilg => ilg.ConvU4());
            AddConversions(convConvertibleTypes, typeof(short), ilg => ilg.ConvI2());
            AddConversions(convConvertibleTypes, typeof(ushort), ilg => ilg.ConvU2());
            AddConversions(convConvertibleTypes, typeof(sbyte), ilg => ilg.ConvI1());
            AddConversions(convConvertibleTypes, typeof(byte), ilg => ilg.ConvU1());
            AddConversions(convConvertibleTypes, typeof(double), ilg => ilg.ConvR8());
            AddConversions(convConvertibleTypes, typeof(float), ilg => ilg.ConvR4());
            foreach (var m in GetType().GetMethods())
            {
                if (!m.IsStatic) continue;
                if (!m.IsPublic) continue;
                if (!m.Name.StartsWith("Convert", StringComparison.Ordinal)) continue;
                if (m.ContainsGenericParameters) continue;
                var parameterInfos = m.GetParameters();
                if (parameterInfos.Length != 1) continue;
                var fromType = parameterInfos[0].ParameterType;
                var closuredMethodInfo = m;
                _conversions[Tuple.Create(fromType, m.ReturnType)] = ilg => ilg.Call(closuredMethodInfo);
            }
        }

        void AddConversions(IEnumerable<Type> fromList, Type to, Action<IILGen> generator)
        {
            foreach (var from in fromList)
            {
                _conversions[Tuple.Create(from, to)] = generator;
            }
        }

        public virtual Action<IILGen> GenerateConversion(Type from, Type to)
        {
            if (from == to) return ilg => { };
            if (!from.IsValueType && to == typeof(object))
            {
                return i => i.Castclass(to);
            }
            if (from == typeof(object) && !to.IsValueType)
            {
                return i => i.Isinst(to);
            }
            Action<IILGen> generator;
            if (_conversions.TryGetValue(new Tuple<Type, Type>(from, to), out generator))
            {
                return generator;
            }
            if (from.IsEnum && to.IsEnum) return GenerateEnum2EnumConversion(from, to);
            return null;
        }

        Action<IILGen> GenerateEnum2EnumConversion(Type from, Type to)
        {
            var fromcfg = new EnumFieldHandler.EnumConfiguration(from);
            var tocfg = new EnumFieldHandler.EnumConfiguration(to);
            if (fromcfg.IsSubsetOf(tocfg))
            {
                return GenerateConversion(from.GetEnumUnderlyingType(), to.GetEnumUnderlyingType());
            }
            return null;
        }

        public static string Convert2String(double value)
        {
            return value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        public static string Convert2String(bool value)
        {
            return value ? "1" : "0";
        }

        public static string Convert2String(long value)
        {
            return value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        public static string Convert2String(ulong value)
        {
            return value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        public static string Convert2String(decimal value)
        {
            return value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        public static decimal Convert2Decimal(long value)
        {
            return new decimal(value);
        }

        public static decimal Convert2Decimal(ulong value)
        {
            return new decimal(value);
        }

        public static decimal Convert2Decimal(int value)
        {
            return new decimal(value);
        }

        public static decimal Convert2Decimal(uint value)
        {
            return new decimal(value);
        }

        public static decimal Convert2Decimal(double value)
        {
            return new decimal(value);
        }

        public static decimal Convert2Decimal(float value)
        {
            return new decimal(value);
        }

        public static bool Convert2Bool(int value)
        {
            return value != 0;
        }

        public static byte[] Convert2Bytes(ByteBuffer buffer)
        {
            return buffer.ToByteArray();
        }

        public static ByteBuffer Convert2ByteBuffer(byte[] bytes)
        {
            return ByteBuffer.NewAsync(bytes);
        }

        public static string? Convert2String(Version? version)
        {
            return version?.ToString();
        }

        public static Version? Convert2Version(string? value)
        {
            Version.TryParse(value, out var result);
            return result;
        }

        public static EncryptedString Convert2EncryptedString(string? secret) => secret;
        public static string? Convert2String(EncryptedString secret) => secret;
    }
}
