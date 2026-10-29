using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BTDB.Buffer;
using BTDB.EventStore2Layer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

/// A damaged count of a list, dictionary or array in an event used to size what it was read into: past int.MaxValue
/// it was a negative capacity or length and deserializing failed with ArgumentOutOfRangeException. The count only
/// hints a capacity now, and an array declaring more items than its data holds is reported before it is allocated.
public class EventDamagedCollectionCountTest
{
    [BTDB.Generate]
    public class Ev
    {
        public List<int>? Items { get; set; }
        public Dictionary<int, int>? Map { get; set; }
        public int[]? Array { get; set; }
    }

    [Fact]
    public void DamagedCollectionCountFailsAsDamage()
    {
        var serializer = new EventSerializer();
        var deserializer = new EventDeserializer();
        var ev = new Ev
        {
            Items = [100, 100, 100, 100, 100], Map = new() { [1] = 2, [3] = 4, [5] = 7, [7] = 8, [9] = 10 },
            Array = [9, 9, 9, 9, 9]
        };
        var data = serializer.Serialize(out var hasMetadata, ev).ToAsyncSafe();
        if (hasMetadata)
        {
            serializer.ProcessMetadataLog(data);
            deserializer.ProcessMetadataLog(data);
            data = serializer.Serialize(out _, ev).ToAsyncSafe();
        }

        var original = data.ToByteArray();
        var huge = new MemWriter();
        huge.WriteVUInt32((1u << 31) + 2);
        var hugeBytes = huge.GetSpan().ToArray();
        var unexpected = new List<string>();
        // Counts are stored plus one, so all three are 6; replacing each byte of 6 in turn damages each of them.
        for (var at = 0; at < original.Length; at++)
        {
            if (original[at] != 6) continue;
            var damaged = original[..at].Concat(hugeBytes).Concat(original[(at + 1)..]).ToArray();
            try
            {
                deserializer.Deserialize(out _, ByteBuffer.NewAsync(damaged));
            }
            catch (Exception error) when (error is InvalidDataException or EndOfStreamException or BTDBException)
            {
            }
            catch (Exception error)
            {
                unexpected.Add($"at {at}: {error.GetType().Name}: {error.Message}");
            }
        }

        Assert.True(unexpected.Count == 0, string.Join("\n", unexpected));
    }
}
