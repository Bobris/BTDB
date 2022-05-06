using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.IL;

namespace BTDB.FieldHandler;

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

    public virtual Action<IILGen>? GenerateConversion(Type from, Type to)
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
        if (_conversions.TryGetValue(new(from, to), out generator))
        {
            return generator;
        }
        if (from.IsEnum && to.IsEnum) return GenerateEnum2EnumConversion(from, to);
        if (Nullable.GetUnderlyingType(to) == from)
        {
            var res = GenerateToNullableConversion(from, to);
            _conversions.Add(new(from, to), res);
            return res;
        }
        if (Nullable.GetUnderlyingType(to) is { } underTo && GenerateConversion(from, underTo) is { } conv)
        {
            Action<IILGen> res = il =>
            {
                conv.Invoke(il);
                GenerateConversion(underTo, to)(il);
            };
            _conversions.Add(new(from, to), res);
            return res;
        }
        var toIList = to.SpecializationOf(typeof(IList<>));
        if (toIList is { } && GenerateConversion(from, toIList.GenericTypeArguments[0]) is { } itemConversion)
        {
            var itemType = toIList.GenericTypeArguments[0];
            return il =>
            {
                itemConversion.Invoke(il);
                var listType = typeof(List<>).MakeGenericType(itemType);
                if (itemType.IsValueType)
                {
                    var local = il.DeclareLocal(itemType);
                    var localList = il.DeclareLocal(listType);
                    il
                        .Stloc(local)
                        .LdcI4(1)
                        .Newobj(listType.GetConstructors().First(c =>
                            c.GetParameters() is { Length: 1 } p && p[0].ParameterType == typeof(int)))
                        .Stloc(localList)
                        .Ldloc(localList)
                        .Ldloc(local)
                        .Call(listType.GetMethod(nameof(List<int>.Add), new[] { itemType })!)
                        .Ldloc(localList)
                        .Castclass(to);
                }
                else
                {
                    var local = il.DeclareLocal(itemType);
                    var localList = il.DeclareLocal(listType);
                    var finishLabel = il.DefineLabel();
                    var emptyListLabel = il.DefineLabel();
                    il
                        .Stloc(local)
                        .Ldloc(local)
                        .BrfalseS(emptyListLabel)
                        .LdcI4(1)
                        .Newobj(listType.GetConstructors().First(c =>
                            c.GetParameters() is { Length: 1 } p && p[0].ParameterType == typeof(int)))
                        .Stloc(localList)
                        .Ldloc(localList)
                        .Ldloc(local)
                        .Call(listType.GetMethod(nameof(List<int>.Add), new[] { itemType })!)
                        .Ldloc(localList)
                        .Br(finishLabel)
                        .Mark(emptyListLabel)
                        .LdcI4(0)
                        .Newobj(listType.GetConstructors().First(c =>
                            c.GetParameters() is { Length: 1 } p && p[0].ParameterType == typeof(int)))
                        .Mark(finishLabel)
                        .Castclass(to);
                }
            };
        }
        var toDict = to.IsGenericType && to.GetGenericTypeDefinition() == typeof(Dictionary<,>);
        var fromIDict = from.IsGenericType && from.GetGenericTypeDefinition() == typeof(IDictionary<,>);
        if (fromIDict && toDict)
        {
            var fromKV = from.GetGenericArguments();
            var toKV = to.GetGenericArguments();
            if (GenerateConversion(fromKV[0], toKV[0]) is { } keyConversion && GenerateConversion(fromKV[1], toKV[1]) is
                    { } valueConversion)
            {
                return ilGenerator =>
                {
                    var realFinish = ilGenerator.DefineLabel();
                    var finish = ilGenerator.DefineLabel();
                    var next = ilGenerator.DefineLabel();
                    var localValue = ilGenerator.DeclareLocal(from);
                    var localTo = ilGenerator.DeclareLocal(to);
                    var localToKey = ilGenerator.DeclareLocal(toKV[0]);
                    var localToValue = ilGenerator.DeclareLocal(toKV[1]);
                    var typeAsICollection = from.GetInterface("ICollection`1");
                    var typeAsIEnumerable = from.GetInterface("IEnumerable`1");
                    var getEnumeratorMethod = typeAsIEnumerable!.GetMethod("GetEnumerator");
                    var typeAsIEnumerator = getEnumeratorMethod!.ReturnType;
                    var typeKeyValuePair = typeAsICollection!.GetGenericArguments()[0];
                    var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
                    var localPair = ilGenerator.DeclareLocal(typeKeyValuePair);
                    ilGenerator
                        .Stloc(localValue)
                        .Newobj(to.GetConstructor(Array.Empty<Type>())!)
                        .Stloc(localTo)
                        .Ldloc(localValue)
                        .Callvirt(getEnumeratorMethod)
                        .Stloc(localEnumerator)
                        .Try()
                        .Mark(next)
                        .Ldloc(localEnumerator)
                        .Callvirt(() => default(IEnumerator).MoveNext())
                        .Brfalse(finish)
                        .Ldloc(localEnumerator)
                        .Callvirt(typeAsIEnumerator.GetProperty("Current")!.GetGetMethod()!)
                        .Stloc(localPair)
                        .Ldloca(localPair)
                        .Call(typeKeyValuePair.GetProperty("Key")!.GetGetMethod()!)
                        .Do(keyConversion)
                        .Stloc(localToKey)
                        .Ldloca(localPair)
                        .Call(typeKeyValuePair.GetProperty("Value")!.GetGetMethod()!)
                        .Do(valueConversion)
                        .Stloc(localToValue)
                        .Ldloc(localTo)
                        .Ldloc(localToKey)
                        .Ldloc(localToValue)
                        .Callvirt(to.GetMethod("Add", BindingFlags.Public | BindingFlags.Instance, toKV)!)
                        .Br(next)
                        .Mark(finish)
                        .Finally()
                        .Ldloc(localEnumerator)
                        .Callvirt(() => default(IDisposable).Dispose())
                        .EndTry()
                        .Mark(realFinish)
                        .Ldloc(localTo);
                };
            }
        }

        return null;
    }

    Action<IILGen> GenerateToNullableConversion(Type from, Type to)
    {
        return il =>
        {
            il.Newobj(to.GetConstructor(new[] { from })!);
        };
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
