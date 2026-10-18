using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.ODBLayer;

namespace BTDBTest;

/// Creates canonical textual representation of object graph, which is independent on concrete collection types
/// and on whether object was deserialized into its CLR type or as dynamic object.
public static class ObjectGraphDumper
{
    public static string Dump(object? obj)
    {
        var sb = new StringBuilder();
        Dump(sb, obj, 0);
        return sb.ToString();
    }

    static void Dump(StringBuilder sb, object? obj, int depth)
    {
        if (depth > 30) throw new InvalidOperationException("Too deep object graph");
        switch (obj)
        {
            case null:
                sb.Append("null");
                return;
            case string s:
                sb.Append('"').Append(s).Append('"');
                return;
            case byte[] bytes:
                sb.Append("0x").Append(Convert.ToHexString(bytes));
                return;
            case ReadOnlyMemory<byte> rom:
                sb.Append("0x").Append(Convert.ToHexString(rom.Span));
                return;
            case ByteBuffer bb:
                sb.Append("0x").Append(Convert.ToHexString(bb.AsSyncReadOnlySpan()));
                return;
            case EncryptedString es:
                sb.Append("enc\"").Append(es.Secret).Append('"');
                return;
            case DateTime dt:
                sb.Append(dt.ToString("O", CultureInfo.InvariantCulture));
                return;
            case DateTimeOffset dto:
                sb.Append(dto.ToString("O", CultureInfo.InvariantCulture));
                return;
            case EnumTypeDescriptor.DynamicEnum:
                sb.Append(obj);
                return;
            case ObjectTypeDescriptor.DynamicObject dynamicObject:
                DumpMembers(sb, dynamicObject!, depth);
                return;
        }

        var type = obj.GetType();
        if (obj is IFormattable formattable && (type.IsPrimitive || type.IsEnum || obj is decimal || obj is Guid ||
                                                obj is TimeSpan))
        {
            sb.Append(formattable.ToString(null, CultureInfo.InvariantCulture));
            return;
        }

        if (obj is bool or Version or System.Net.IPAddress or Microsoft.Extensions.Primitives.StringValues)
        {
            sb.Append(obj);
            return;
        }

        if (obj is TupleTypeDescriptor.DynamicTuple dynamicTuple)
        {
            sb.Append('(');
            var firstItem = true;
            foreach (var item in dynamicTuple)
            {
                if (!firstItem) sb.Append(", ");
                firstItem = false;
                Dump(sb, item, depth + 1);
            }

            sb.Append(')');
            return;
        }

        if (obj is ITuple tuple)
        {
            sb.Append('(');
            for (var i = 0; i < tuple.Length; i++)
            {
                if (i > 0) sb.Append(", ");
                Dump(sb, tuple[i], depth + 1);
            }

            sb.Append(')');
            return;
        }

        if (obj is IDictionary dictionary)
        {
            var items = new List<(object? Key, object? Value)>();
            foreach (DictionaryEntry entry in dictionary)
                items.Add((entry.Key, entry.Value));
            DumpDictionary(sb, items, depth);
            return;
        }

        if (IsKeyValueEnumerable(type))
        {
            var items = new List<(object? Key, object? Value)>();
            foreach (var item in (IEnumerable)obj)
            {
                var itemType = item!.GetType();
                items.Add((itemType.GetProperty("Key")!.GetValue(item), itemType.GetProperty("Value")!.GetValue(item)));
            }

            DumpDictionary(sb, items, depth);
            return;
        }

        if (obj is IEnumerable enumerable)
        {
            sb.Append('[');
            var first = true;
            foreach (var item in enumerable)
            {
                if (!first) sb.Append(", ");
                first = false;
                Dump(sb, item, depth + 1);
            }

            sb.Append(']');
            return;
        }

        var members = new List<KeyValuePair<string, object?>>();
        foreach (var prop in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (prop.GetIndexParameters().Length != 0) continue;
            if (prop.GetMethod == null) continue;
            if (prop.GetCustomAttributes(typeof(NotStoredAttribute), true).Length != 0) continue;
            members.Add(new(prop.Name, prop.GetValue(obj)));
        }

        DumpMembers(sb, members, depth);
    }

    static void DumpMembers(StringBuilder sb, IEnumerable<KeyValuePair<string, object?>> members, int depth)
    {
        var list = new List<KeyValuePair<string, object?>>(members);
        list.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
        sb.Append('{');
        var first = true;
        foreach (var (key, value) in list)
        {
            if (!first) sb.Append(", ");
            first = false;
            sb.Append(key).Append(": ");
            Dump(sb, value, depth + 1);
        }

        sb.Append('}');
    }

    static void DumpDictionary(StringBuilder sb, List<(object? Key, object? Value)> items, int depth)
    {
        var dumped = items.ConvertAll(i => (Key: Dump(i.Key), Value: Dump(i.Value)));
        dumped.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
        sb.Append("{|");
        var first = true;
        foreach (var (key, value) in dumped)
        {
            if (!first) sb.Append(", ");
            first = false;
            sb.Append(key).Append(" => ").Append(value);
        }

        sb.Append("|}");
    }

    static bool IsKeyValueEnumerable(Type type)
    {
        foreach (var i in type.GetInterfaces())
        {
            if (!i.IsGenericType || i.GetGenericTypeDefinition() != typeof(IEnumerable<>)) continue;
            var arg = i.GetGenericArguments()[0];
            if (arg.IsGenericType && arg.GetGenericTypeDefinition() == typeof(KeyValuePair<,>)) return true;
        }

        return false;
    }
}
