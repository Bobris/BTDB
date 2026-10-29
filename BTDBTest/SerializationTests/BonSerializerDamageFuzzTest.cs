using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.Bon;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest.SerializationTests;

// Damaged BON from BonSerializer may fail to deserialize with any exception, but must neither exhaust memory nor crash.
public class BonSerializerDamageFuzzTest
{
    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 300).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void DamagedBonFailsWithoutRunningAway(int seed)
    {
        var random = new Random(seed);
        var obj = new BonSerializerTest.AllSupportedTypes
        {
            Str = new string('s', random.Next(10)), Int32 = random.Next(), Int64 = random.NextInt64(),
            DateTime = new(2024, 2, 11), Guid = Guid.NewGuid(), Float64 = random.NextDouble(),
            ValueTupleLongString = (random.NextInt64(), "B"), TupleLongString = new(1, "BB"),
            Self = new() { Str = "inner", IntList = [1, 2] }, DoubleArray = [1, 2, 3],
            IntList = Enumerable.Range(0, random.Next(40)).ToList(), UShortSet = [1, 2, 3],
            LongIntDict = Enumerable.Range(0, random.Next(10)).ToDictionary(i => (long)i, i => i)
        };
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, obj);
        var original = builder.FinishAsMemory().ToArray();
        for (var round = 0; round < 50; round++)
        {
            var bytes = (byte[])original.Clone();
            if (random.Next(5) == 0) bytes = bytes[..random.Next(1, bytes.Length)];
            else
                for (var changes = random.Next(1, 4); changes > 0; changes--)
                    bytes[random.Next(bytes.Length)] = (byte)random.Next(256);
            try
            {
                var bon = new Bon(bytes);
                BonSerializerFactory.Deserialize(ref bon);
            }
            catch (Exception error) when (error is not OutOfMemoryException and not InsufficientExecutionStackException)
            {
            }
        }
    }
}
