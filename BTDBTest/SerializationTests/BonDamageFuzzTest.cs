using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BTDB;
using BTDB.Bon;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest.SerializationTests;

/// Damaged Bon of generated classes, with random bytes changed or the end cut off, deserializes, or dumps to JSON, or
/// fails with an exception; it never crashes the process or loops forever.
public class BonDamageFuzzTest
{
    [Generate]
    public class DamagedItem
    {
        public string? Text { get; set; }
        public int Number { get; set; }
    }

    [Generate]
    public class DamagedRoot
    {
        public string? Name { get; set; }
        public long Long { get; set; }
        public List<DamagedItem>? Items { get; set; }
        public Dictionary<string, DamagedItem>? Map { get; set; }
        public (int, string) Tuple { get; set; }
        public byte[]? Bytes { get; set; }
        public double Double { get; set; }
        public DamagedItem? One { get; set; }
        public object? Anything { get; set; }
    }

    public static TheoryData<int> Seeds() => new(Enumerable.Range(1, 8));

    [Theory]
    [MemberData(nameof(Seeds))]
    public void DamagedBonDeserializesOrThrows(int seed)
    {
        var random = new Random(seed);
        var value = new DamagedRoot
        {
            Name = "name", Long = 1L << 40, Items = [new() { Text = "a", Number = 1 }, new() { Number = 2 }],
            Map = new() { ["k"] = new() { Text = "m", Number = 3 } }, Tuple = (4, "t"), Bytes = [1, 2, 3],
            Double = 1.5, One = new() { Text = "o" }, Anything = new List<object?> { 1L, "x", new DamagedItem() }
        };
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, value);
        var original = builder.FinishAsMemory().ToArray();
        var unexpected = new List<string>();
        for (var round = 0; round < 2000; round++)
        {
            var bytes = (byte[])original.Clone();
            if (random.Next(4) == 0) bytes = bytes[..random.Next(bytes.Length)];
            else
                for (var changes = random.Next(1, 4); changes > 0; changes--)
                    bytes[random.Next(bytes.Length)] = (byte)random.Next(256);
            try
            {
                var bon = new Bon(bytes);
                BonSerializerFactory.Deserialize(ref bon);
            }
            catch (Exception error) when (error is InvalidDataException or EndOfStreamException or NotSupportedException
                                              or InsufficientExecutionStackException)
            {
            }
            catch (Exception error) when (error.GetType().Name == "DebugAssertException")
            {
            }
            catch (Exception error)
            {
                unexpected.Add($"round {round}: {error.GetType().Name}: {error.Message}");
            }

            try
            {
                var bon = new Bon(bytes);
                using var json = new System.Text.Json.Utf8JsonWriter(new MemoryStream());
                bon.DumpToJson(json);
            }
            // The JSON writer refuses what damage leaves, such as a key without a value, or nesting past its depth.
            catch (Exception error) when (error is InvalidDataException or EndOfStreamException or NotSupportedException
                                              or InvalidOperationException or ArgumentException)
            {
            }
            catch (Exception error) when (error.GetType().Name == "DebugAssertException")
            {
            }
            catch (Exception error)
            {
                unexpected.Add($"round {round}: {error.GetType().Name}: {error.Message}");
            }
        }

        Assert.True(unexpected.Count == 0, string.Join("\n", unexpected.Take(10)));
    }
}
