using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace BTDB.Replication;

/// <summary>Typed leader.json access. A malformed record is invalid cluster configuration (InvalidDataException),
/// never an unrelated runtime failure such as InvalidOperationException or NullReferenceException.</summary>
internal static class LeaderJson
{
    public static JsonObject Parse(string json)
    {
        try
        {
            return JsonNode.Parse(json) as JsonObject ?? throw new InvalidDataException("Leader record must be a JSON object.");
        }
        catch (JsonException error) { throw new InvalidDataException("Leader record is not valid JSON.", error); }
    }

    public static string? OptionalString(JsonObject json, string name) => Optional<string>(json, name);
    public static int? OptionalInt32(JsonObject json, string name) => json[name] == null ? null : Optional<int>(json, name);
    public static ulong OptionalUInt64(JsonObject json, string name) => Optional<ulong>(json, name);

    public static ulong RequiredUInt64(JsonObject json, string name) => json[name] == null
        ? throw Missing(name) : Optional<ulong>(json, name);

    public static string RequiredString(JsonObject json, string name) => Optional<string>(json, name) ?? throw Missing(name);

    public static string[]? OptionalNames(JsonObject json, string name)
    {
        if (json[name] is not { } node) return null;
        if (node is not JsonArray names) throw Invalid(name, null);
        return names.Select(n => n is JsonValue value && value.TryGetValue<string>(out var text) ? text : throw Invalid(name, null))
            .ToArray();
    }

    static T? Optional<T>(JsonObject json, string name)
    {
        if (json[name] is not { } node) return default;
        try { return node.GetValue<T>(); }
        catch (Exception error) when (error is InvalidOperationException or FormatException or OverflowException)
        { throw Invalid(name, error); }
    }

    static InvalidDataException Missing(string name) => new($"Leader record lacks required field '{name}'.");
    static InvalidDataException Invalid(string name, Exception? error) => new($"Leader record field '{name}' has an invalid value.", error);
}
