using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json.Serialization.Metadata;
using BTDB.Replication.EventSourcing;
using DiffEngine;
using Xunit;

namespace BTDB.BackendActions.Test;

/// <summary>Baselines of what other nodes and stored data depend on: a change to a baseline is a change of the public
/// API, of a replicated or stored format, or of the format nodes exchange during a rolling upgrade.</summary>
public class ApprovalTest
{
    static readonly Assembly Package = typeof(BackendActions).Assembly;

    [ModuleInitializer]
    internal static void Init()
    {
        if (Environment.GetEnvironmentVariable("CI") is { Length: > 0 } ||
            Environment.GetEnvironmentVariable("CLAUDECODE") is { Length: > 0 })
            DiffRunner.Disabled = true;
    }

    [Fact]
    public Task PublicApi()
    {
        var text = new StringBuilder();
        foreach (var type in Package.GetExportedTypes().OrderBy(t => t.FullName, StringComparer.Ordinal))
            DescribeApi(text, type);
        return Verify(text.ToString());
    }

    [Fact]
    public Task Events()
    {
        var events = Package.GetExportedTypes()
            .Where(t => t.IsClass && !t.IsAbstract && typeof(Event).IsAssignableFrom(t));
        return Verify(DescribeStored(events, type => "event " + type.FullName));
    }

    [Fact]
    public Task Relations()
    {
        var relations = BackendActions.RelationTypes.ToDictionary(RowType);
        return Verify(DescribeStored(relations.Keys,
            type => $"relation {relations[type].Name}{Attributes(relations[type].GetCustomAttributesData())} of {type.FullName}"));
    }

    [Fact]
    public Task Exchange()
    {
        var text = new StringBuilder();
        var seen = new HashSet<Type>();
        var pending = new Queue<Type>([typeof(ExchangeRequest), typeof(ExchangeResponse), typeof(ActionsOverview)]);
        while (pending.TryDequeue(out var type))
        {
            if (!seen.Add(type)) continue;
            var info = ExchangeJson.Default.GetTypeInfo(type)!;
            if (info.Kind == JsonTypeInfoKind.Enumerable)
            {
                pending.Enqueue(info.ElementType!);
                continue;
            }
            if (type.IsEnum)
            {
                DescribeEnum(text, type);
                continue;
            }
            if (info.Kind != JsonTypeInfoKind.Object) continue;
            text.Append("json ").AppendLine(type.Name);
            foreach (var property in info.Properties.OrderBy(p => p.Name, StringComparer.Ordinal))
            {
                text.Append("  ").Append(property.Name).Append(": ").AppendLine(Name(property.PropertyType));
                pending.Enqueue(property.PropertyType);
            }
        }
        return Verify(text.ToString());
    }

    static Task Verify(string text) => Verifier.Verify(text);

    static Type RowType(Type relation) => relation.GetInterfaces()
        .Single(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(BTDB.ODBLayer.IRelation<>))
        .GetGenericArguments()[0];

    /// <summary>Stored properties of the roots and of every package type they reach, with enum values, which are
    /// stored as numbers.</summary>
    static string DescribeStored(IEnumerable<Type> roots, Func<Type, string> header)
    {
        var text = new StringBuilder();
        var seen = new HashSet<Type>();
        var rootSet = roots.ToHashSet();
        var pending = new Queue<Type>(rootSet.OrderBy(t => t.FullName, StringComparer.Ordinal));
        while (pending.TryDequeue(out var type))
        {
            if (type.IsGenericType)
            {
                foreach (var argument in type.GetGenericArguments()) pending.Enqueue(argument);
                continue;
            }
            if (type.Assembly != Package || !seen.Add(type)) continue;
            if (type.IsEnum)
            {
                DescribeEnum(text, type);
                continue;
            }
            text.AppendLine(rootSet.Contains(type) ? header(type) : "class " + type.FullName);
            // Properties of base classes from other packages, such as an event's EventId, are theirs to approve.
            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                         .Where(p => p is { CanRead: true, CanWrite: true } && p.DeclaringType!.Assembly == Package)
                         .OrderBy(p => p.Name, StringComparer.Ordinal))
            {
                text.Append("  ").Append(property.Name).Append(": ").Append(Name(property.PropertyType))
                    .AppendLine(Attributes(property.GetCustomAttributesData()));
                pending.Enqueue(property.PropertyType);
            }
        }
        return text.ToString();
    }

    static void DescribeEnum(StringBuilder text, Type type)
    {
        text.Append("enum ").Append(type.FullName).Append(" : ").AppendLine(Name(Enum.GetUnderlyingType(type)));
        EnumValues(text, type);
    }

    static void EnumValues(StringBuilder text, Type type)
    {
        foreach (var name in Enum.GetNames(type))
            text.Append("  ").Append(name).Append(" = ")
                .AppendLine(Value(Convert.ChangeType(Enum.Parse(type, name), Enum.GetUnderlyingType(type))));
    }

    /// <summary>BTDB attributes, which decide keys and stored names.</summary>
    static string Attributes(IEnumerable<CustomAttributeData> attributes)
    {
        var text = new StringBuilder();
        foreach (var attribute in attributes.Where(a => a.AttributeType.Namespace?.StartsWith("BTDB") == true))
        {
            text.Append(" [").Append(attribute.AttributeType.Name.Replace("Attribute", ""));
            if (attribute.ConstructorArguments.Count > 0)
                text.Append('(').Append(string.Join(", ", attribute.ConstructorArguments.Select(a => Value(a.Value))))
                    .Append(')');
            text.Append(']');
        }
        return text.ToString();
    }

    static void DescribeApi(StringBuilder text, Type type)
    {
        var nullability = new NullabilityInfoContext();
        if (type.IsSubclassOf(typeof(MulticastDelegate)))
        {
            var invoke = type.GetMethod("Invoke")!;
            text.Append("public delegate ").Append(Name(invoke.ReturnType, nullability.Create(invoke.ReturnParameter)))
                .Append(' ').Append(Name(type)).Append(Parameters(invoke, nullability)).AppendLine();
            return;
        }
        var kind = type.IsInterface ? "interface"
            : type.IsEnum ? "enum"
            : type.IsValueType ? type.GetCustomAttribute<IsReadOnlyAttribute>() != null ? "readonly struct" : "struct"
            : type is { IsAbstract: true, IsSealed: true } ? "static class"
            : type.IsAbstract ? "abstract class"
            : type.IsSealed ? "sealed class" : "class";
        text.Append("public ").Append(kind).Append(' ').Append(type.Namespace).Append('.').Append(Name(type));
        var bases = new List<string>();
        if (type.BaseType is { } baseType && baseType != typeof(object) && baseType != typeof(ValueType) &&
            baseType != typeof(Enum))
            bases.Add(Name(baseType));
        if (!type.IsEnum)
            bases.AddRange(type.GetInterfaces().Select(i => Name(i)).Order(StringComparer.Ordinal));
        if (bases.Count > 0) text.Append(" : ").Append(string.Join(", ", bases));
        text.AppendLine();
        if (type.IsEnum)
        {
            EnumValues(text, type);
            return;
        }

        const BindingFlags all = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance |
                                 BindingFlags.Static | BindingFlags.DeclaredOnly;
        var members = new List<(string Name, string Text)>();
        foreach (var constructor in type.GetConstructors(all).Where(IsVisible))
            members.Add((".ctor", Modifiers(constructor, type) + "ctor" + Parameters(constructor, nullability)));
        foreach (var method in type.GetMethods(all).Where(m => IsVisible(m) && !m.IsSpecialName && !m.Name.StartsWith('<')))
        {
            var generic = method.IsGenericMethodDefinition
                ? "<" + string.Join(", ", method.GetGenericArguments().Select(a => a.Name)) + ">"
                : "";
            members.Add((method.Name, Modifiers(method, type) + "method " +
                                      Name(method.ReturnType, nullability.Create(method.ReturnParameter)) + " " +
                                      method.Name + generic + Parameters(method, nullability)));
        }
        foreach (var property in type.GetProperties(all))
        {
            var accessors = new[] { property.GetMethod, property.SetMethod }.Where(m => m != null && IsVisible(m))
                .ToList();
            if (accessors.Count == 0 || property.Name.StartsWith('<') || property.Name == "EqualityContract") continue;
            var set = property.SetMethod is { } setter && IsVisible(setter)
                ? setter.ReturnParameter.GetRequiredCustomModifiers().Contains(typeof(IsExternalInit)) ? " init;" : " set;"
                : "";
            var indexer = property.GetIndexParameters() is { Length: > 0 } index
                ? "[" + string.Join(", ", index.Select(p => Name(p.ParameterType, nullability.Create(p)) + " " + p.Name)) + "]"
                : "";
            members.Add((property.Name, Modifiers(accessors[0]!, type) + "property " +
                                        Name(property.PropertyType, nullability.Create(property)) + " " +
                                        property.Name + indexer + " {" +
                                        (property.GetMethod is { } getter && IsVisible(getter) ? " get;" : "") + set +
                                        " }"));
        }
        foreach (var field in type.GetFields(all).Where(f => (f.IsPublic || f.IsFamily || f.IsFamilyOrAssembly) &&
                                                            !f.Name.StartsWith('<')))
            members.Add((field.Name, (field.IsLiteral ? "const " : field.IsStatic ? "static " : "") +
                                     (field.IsInitOnly ? "readonly " : "") + "field " +
                                     Name(field.FieldType, nullability.Create(field)) + " " + field.Name));
        foreach (var ev in type.GetEvents(all).Where(e => e.AddMethod is { } add && IsVisible(add)))
            members.Add((ev.Name, "event " + Name(ev.EventHandlerType!) + " " + ev.Name));
        foreach (var member in members.OrderBy(m => m.Name, StringComparer.Ordinal)
                     .ThenBy(m => m.Text, StringComparer.Ordinal))
            text.Append("  ").AppendLine(member.Text);
    }

    static bool IsVisible(MethodBase method) => method.IsPublic || method.IsFamily || method.IsFamilyOrAssembly;

    static string Modifiers(MethodBase method, Type type)
    {
        var protectedPrefix = method.IsPublic ? "" : "protected ";
        if (method.IsStatic) return protectedPrefix + "static ";
        if (type.IsInterface || method is not MethodInfo info) return protectedPrefix;
        if (info.IsAbstract) return protectedPrefix + "abstract ";
        if (info.GetBaseDefinition() != info) return protectedPrefix + "override ";
        if (info.IsVirtual && !info.IsFinal) return protectedPrefix + "virtual ";
        return protectedPrefix;
    }

    static string Parameters(MethodBase method, NullabilityInfoContext nullability) =>
        "(" + string.Join(", ", method.GetParameters().Select(p =>
        {
            var prefix = p.IsOut ? "out " : p.ParameterType.IsByRef ? p.IsIn ? "in " : "ref " : "";
            if (p.GetCustomAttribute<ParamArrayAttribute>() != null ||
                p.GetCustomAttribute<ParamCollectionAttribute>() != null)
                prefix = "params ";
            if (!p.HasDefaultValue) return prefix + Name(p.ParameterType, nullability.Create(p)) + " " + p.Name;
            // A struct's default value reads as null.
            var isDefault = p.DefaultValue == null && p.ParameterType.IsValueType &&
                            Nullable.GetUnderlyingType(p.ParameterType) == null;
            return prefix + Name(p.ParameterType, nullability.Create(p)) + " " + p.Name + " = " +
                   (isDefault ? "default" : Value(p.DefaultValue));
        })) + ")";

    static string Value(object? value) => value switch
    {
        null => "null",
        string s => "\"" + s + "\"",
        bool b => b ? "true" : "false",
        IFormattable f => f.ToString(null, System.Globalization.CultureInfo.InvariantCulture),
        _ => value.ToString()!
    };

    static string Name(Type type, NullabilityInfo? nullability = null)
    {
        if (type.IsByRef) return Name(type.GetElementType()!, nullability?.ElementType ?? nullability);
        if (Nullable.GetUnderlyingType(type) is { } underlying)
            return Name(underlying, nullability?.GenericTypeArguments.FirstOrDefault()) + "?";
        var suffix = !type.IsValueType && nullability?.ReadState == NullabilityState.Nullable ? "?" : "";
        if (type.IsArray) return Name(type.GetElementType()!, nullability?.ElementType) + "[]" + suffix;
        if (type.IsGenericParameter) return type.Name + suffix;
        if (Keywords.TryGetValue(type, out var keyword)) return keyword + suffix;
        var prefix = type.IsNested ? Name(type.DeclaringType!) + "." : "";
        if (!type.IsGenericType) return prefix + type.Name + suffix;
        var arguments = type.GetGenericArguments();
        var own = type.IsNested ? arguments.Skip(type.DeclaringType!.GetGenericArguments().Length).ToArray() : arguments;
        var offset = arguments.Length - own.Length;
        var name = type.Name.Split('`')[0];
        if (own.Length == 0) return prefix + name + suffix;
        return prefix + name + "<" + string.Join(", ", own.Select((a, i) =>
            Name(a, nullability?.GenericTypeArguments.ElementAtOrDefault(offset + i)))) + ">" + suffix;
    }

    static readonly Dictionary<Type, string> Keywords = new()
    {
        [typeof(void)] = "void", [typeof(object)] = "object", [typeof(string)] = "string", [typeof(bool)] = "bool",
        [typeof(byte)] = "byte", [typeof(sbyte)] = "sbyte", [typeof(short)] = "short", [typeof(ushort)] = "ushort",
        [typeof(int)] = "int", [typeof(uint)] = "uint", [typeof(long)] = "long", [typeof(ulong)] = "ulong",
        [typeof(float)] = "float", [typeof(double)] = "double", [typeof(decimal)] = "decimal"
    };
}
