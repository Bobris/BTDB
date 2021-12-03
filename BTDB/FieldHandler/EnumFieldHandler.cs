using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BTDB.IL;
using BTDB.StreamLayer;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Primitives;

namespace BTDB.FieldHandler;

public class EnumFieldHandler : IFieldHandler
{
    readonly byte[] _configuration;
    readonly bool _signed;
    Type? _enumType;

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
            var members = enumType.GetMembers(BindingFlags.Static | BindingFlags.Public);
            Debug.Assert(members.Length == _names.Length);
            for (int i = 0; i < members.Length; i++)
            {
                var a = members[i].GetCustomAttributes<PersistedNameAttribute>().FirstOrDefault();
                if (a != null) _names[i] = a.Name;
            }
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
            var reader = new SpanReader(configuration);
            var header = reader.ReadVUInt32();
            _signed = (header & 1) != 0;
            _flags = (header & 2) != 0;
            var count = header >> 2;
            _names = new string[count];
            _values = new ulong[count];
            for (var i = 0; i < count; i++) Names[i] = reader.ReadString()!;
            if (_signed)
            {
                for (var i = 0; i < count; i++) Values[i] = (ulong)reader.ReadVInt64();
            }
            else
            {
                for (var i = 0; i < count; i++) Values[i] = reader.ReadVUInt64();
            }
        }

        public bool Signed => _signed;

        public bool Flags => _flags;

        public string[] Names => _names;

        public ulong[] Values => _values;

        public byte[] ToConfiguration()
        {
            var writer = new SpanWriter();
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
            return writer.GetSpan().ToArray();
        }

        public Type ToType()
        {
            var builder = ILBuilder.Instance;
            var literals = new Dictionary<string, object>();
            for (var i = 0; i < Names.Length; i++)
            {
                if (_signed)
                {
                    literals.Add(Names[i], (long)Values[i]);
                }
                else
                {
                    literals.Add(Names[i], Values[i]);
                }
            }
            return builder.NewEnum("EnumByFieldHandler", _signed ? typeof(long) : typeof(ulong), literals);
        }

        public static bool operator ==(EnumConfiguration? left, EnumConfiguration? right)
        {
            if (ReferenceEquals(left, right)) return true;
            if (ReferenceEquals(left, null)) return false;
            return left.Equals(right);
        }

        public static bool operator !=(EnumConfiguration? left, EnumConfiguration? right)
        {
            return !(left == right);
        }

        public bool Equals(EnumConfiguration? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return other._flags.Equals(_flags)
                && _names.SequenceEqual(other._names)
                && _values.SequenceEqual(other._values);
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != typeof(EnumConfiguration)) return false;
            return Equals((EnumConfiguration)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var result = _flags.GetHashCode();
                result = (result * 397) ^ _names.GetHashCode();
                result = (result * 397) ^ _values.GetHashCode();
                return result;
            }
        }

        public bool IsSubsetOf(EnumConfiguration targetCfg)
        {
            if (_flags != targetCfg._flags) return false;
            var targetDict =
                targetCfg.Names.Zip(targetCfg.Values, (k, v) => new KeyValuePair<string, ulong>(k, v))
                .ToDictionary(p => p.Key, p => p.Value);
            for (var i = 0; i < _names.Length; i++)
            {
                if (!targetDict.TryGetValue(_names[i], out var targetValue)) return false;
                if (_values[i] != targetValue) return false;
            }
            return true;
        }

        public bool IsBinaryRepresentationSubsetOf(EnumConfiguration targetCfg)
        {
            var targetSet = targetCfg.Values.ToHashSet();
            return _values.All(v => targetSet.Contains(v));
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

    public static string HandlerName => "Enum";

    public string Name => HandlerName;

    public byte[] Configuration => _configuration;

    public static bool IsCompatibleWith(Type type)
    {
        if (!type.IsEnum) return false;
        var enumUnderlyingType = type.GetEnumUnderlyingType();
        return SignedFieldHandler.IsCompatibleWith(enumUnderlyingType) || UnsignedFieldHandler.IsCompatibleWith(enumUnderlyingType);
    }

    public Type HandledType()
    {
        return _enumType ??= new EnumConfiguration(_configuration).ToType();
    }

    public bool NeedsCtx()
    {
        return false;
    }

    public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        pushReader(ilGenerator);
        Type typeRead;
        if (_signed)
        {
            ilGenerator.Call(typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVInt64))!);
            typeRead = typeof(long);
        }
        else
        {
            ilGenerator.Call(typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVUInt64))!);
            typeRead = typeof(ulong);
        }
        DefaultTypeConvertorGenerator.Instance.GenerateConversion(typeRead, _enumType!.GetEnumUnderlyingType())!(ilGenerator);
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        pushReader(ilGenerator);
        if (_signed)
        {
            ilGenerator.Call(typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVInt64))!);
        }
        else
        {
            ilGenerator.Call(typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVUInt64))!);
        }
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx, Action<IILGen> pushValue)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        if (_signed)
        {
            ilGenerator
                .ConvI8()
                .Call(typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteVInt64))!);
        }
        else
        {
            ilGenerator
                .ConvU8()
                .Call(typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteVUInt64))!);
        }
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        if (typeHandler == this) return this;
        var enumTypeHandler = typeHandler as EnumFieldHandler;
        if (typeHandler == null && type.IsEnum)
        {
            enumTypeHandler = new EnumFieldHandler(type);
            typeHandler = enumTypeHandler;
        }
        if (enumTypeHandler != null && _signed == enumTypeHandler._signed)
        {
            if (new EnumConfiguration(Configuration).IsBinaryRepresentationSubsetOf(new EnumConfiguration(enumTypeHandler.Configuration)))
                return typeHandler;
        }
        logger?.ReportTypeIncompatibility(_enumType, this, type, typeHandler);
        return this;
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        return SpecializeLoadForType(type, null, null);
    }

    public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return IsCompatibleWith(type);
    }

    public NeedsFreeContent FreeContent(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        Skip(ilGenerator, pushReader, pushCtx);
        return NeedsFreeContent.No;
    }

    public override string ToString()
    {
        var ec = new EnumConfiguration(Configuration);
        var sb = new StringBuilder();
        sb.Append("Enum");
        if (_enumType != null)
        {
            sb.Append(' ');
            sb.Append(_enumType.ToSimpleName());
        }
        if (ec.Flags) sb.Append("[Flags]");
        if (!ec.Signed) sb.Append("[Unsigned]");
        sb.Append('{');
        for (var i = 0; i < ec.Names.Length; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append(ec.Names[i]);
            sb.Append('=');
            sb.Append(ec.Values[i]);
        }
        sb.Append('}');
        return sb.ToString();
    }
}
