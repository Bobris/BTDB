using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BTDB;
using BTDB.Buffer;
using BTDB.EventStoreLayer;
using Xunit;
using NoCompressionStrategy = BTDB.EventStoreLayer.NoCompressionStrategy;

namespace BTDBTest;

public class EventStoreFileCoverageTest
{
    [Generate]
    public class Ev
    {
        public string? Text { get; set; }
        public int Num { get; set; }
    }

    /// Storage over plain byte array, which returns only bytes which were really written.
    class ArrayEventFileStorage : IEventFileStorage
    {
        public byte[] Data;

        public ArrayEventFileStorage(byte[] data, uint maxBlockSize = 4096)
        {
            Data = data;
            MaxBlockSize = maxBlockSize;
        }

        public uint MaxBlockSize { get; }
        public ulong MaxFileSize => int.MaxValue;

        public uint Read(ByteBuffer buf, ulong position)
        {
            if (position >= (ulong)Data.Length) return 0;
            var len = Math.Min(buf.Length, Data.Length - (int)position);
            Array.Copy(Data, (int)position, buf.Buffer!, buf.Offset, len);
            return (uint)len;
        }

        public void Write(ByteBuffer buf, ulong position)
        {
            var end = (int)position + buf.Length;
            if (end > Data.Length) Array.Resize(ref Data, end);
            Array.Copy(buf.Buffer!, buf.Offset, Data, (int)position, buf.Length);
        }

        public IEventFileStorage CreateNew(IEventFileStorage previous) => throw new InvalidOperationException();
    }

    static void WriteBlock(byte[] data, int offset, byte blockType, int payloadLength, uint? checksum = null)
    {
        var lenAndType = ((uint)payloadLength << 8) | blockType;
        BitConverter.TryWriteBytes(data.AsSpan(offset + 4), lenAndType);
        var sum = checksum ?? Checksum.CalcFletcher32(data, (uint)offset + 4, (uint)payloadLength + 4);
        BitConverter.TryWriteBytes(data.AsSpan(offset), sum);
    }

    static ReadOnlyEventStore Open(byte[] data) =>
        (ReadOnlyEventStore)new EventStoreManager().OpenReadOnlyStore(new ArrayEventFileStorage(data));

    static void AssertCorrupted(byte[] data)
    {
        var reader = Open(data);
        Assert.Throws<InvalidDataException>(() => reader.ReadFromStartToEnd(new SkippingEventObserver()));
        Assert.True(reader.IsKnownAsCorrupted());
        Assert.False(reader.IsKnownAsAppendable());
    }

    const byte FirstAndLast = 1 | 4;

    [Fact]
    public void EmptyBlockFollowedByZeroesIsValid()
    {
        var data = new byte[64];
        WriteBlock(data, 0, FirstAndLast, 0);
        var reader = Open(data);
        var observer = new CollectingEventObserver();
        reader.ReadFromStartToEnd(observer);
        Assert.False(reader.IsKnownAsCorrupted());
        Assert.True(reader.IsKnownAsAppendable());
        Assert.Equal([], Assert.Single(observer.Events));
    }

    [Fact]
    public void BlockWithWrongChecksumIsCorrupted()
    {
        var data = new byte[64];
        WriteBlock(data, 0, FirstAndLast, 10, checksum: 12345);
        AssertCorrupted(data);
    }

    [Fact]
    public void EmptyNonCompleteBlockIsCorrupted()
    {
        var data = new byte[64];
        WriteBlock(data, 0, 1, 0);
        AssertCorrupted(data);
    }

    [Fact]
    public void TooLongBlockIsCorrupted()
    {
        var data = new byte[64];
        WriteBlock(data, 0, FirstAndLast, 5000, checksum: 1);
        AssertCorrupted(data);
    }

    [Fact]
    public void BlockLongerThanFileIsCorrupted()
    {
        var data = new byte[64];
        WriteBlock(data, 0, FirstAndLast, 100, checksum: 1);
        AssertCorrupted(data);
    }

    [Fact]
    public void LastBlockWithoutFirstBlockIsCorrupted()
    {
        var data = new byte[64];
        WriteBlock(data, 0, 4, 10);
        AssertCorrupted(data);
    }

    [Fact]
    public void BlockWithoutPositionFlagsIsCorrupted()
    {
        var data = new byte[64];
        WriteBlock(data, 0, 0, 10);
        AssertCorrupted(data);
    }

    [Fact]
    public void FinishMarkWithWrongChecksumIsCorrupted()
    {
        var data = new byte[64];
        WriteBlock(data, 0, 4, 0, checksum: 7);
        AssertCorrupted(data);
    }

    [Fact]
    public void NonZeroBytesAfterLastFullHeaderAreCorrupted()
    {
        var data = new byte[4096];
        WriteBlock(data, 0, FirstAndLast, 4082);
        // Last 6 bytes cannot contain header anymore, they must be zero
        data[4093] = 1;
        AssertCorrupted(data);
        data[4093] = 0;
        var reader = Open(data);
        reader.ReadFromStartToEnd(new SkippingEventObserver());
        Assert.False(reader.IsKnownAsCorrupted());
        Assert.True(reader.IsKnownAsAppendable());
    }

    [Fact]
    public void UnfinishedMultiBlockEventIsNotAppendable()
    {
        var data = new byte[64];
        WriteBlock(data, 0, 1, 10);
        var reader = Open(data);
        reader.ReadFromStartToEnd(new SkippingEventObserver());
        Assert.False(reader.IsKnownAsCorrupted());
        Assert.False(reader.IsKnownAsAppendable());
    }

    [Fact]
    public void ReadOnlyStoreRequiresBigEnoughMaxBlockSize()
    {
        Assert.Throws<ArgumentException>(() =>
            new EventStoreManager().OpenReadOnlyStore(new ArrayEventFileStorage([], 1024)));
    }

    [Fact]
    public void StreamEventFileStorageRoundTrip()
    {
        using var stream = new MemoryStream();
        using var storage = new StreamEventFileStorage(stream);
        Assert.Equal(4u * 1024 * 1024, storage.MaxBlockSize);
        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(storage);
        appender.Store(new Ev { Text = "meta" }, [new Ev { Text = "a", Num = 1 }, new Ev { Text = "b", Num = 2 }]);
        Assert.True(stream.Length > 0);
        Assert.Throws<InvalidOperationException>(() => storage.CreateNew(storage));

        var reader = new EventStoreManager().OpenReadOnlyStore(storage);
        var observer = new CollectingEventObserver();
        reader.ReadFromStartToEnd(observer);
        var events = Assert.Single(observer.Events);
        Assert.Equal(["a", "b"], events.Cast<Ev>().Select(e => e.Text));
    }

    [Fact]
    public void StoreSwitchesToNewFileWhenFull()
    {
        var manager = new EventStoreManager { CompressionStrategy = new NoCompressionStrategy() };
        var file = new MemoryEventFileStorage(4096, 8192);
        var appender = manager.AppendToStore(file);
        for (var i = 0; i < 10; i++)
            appender.Store(null, [new Ev { Text = new string('x', 1500), Num = i }]);
        Assert.NotSame(file, appender.CurrentFileStorage);
        var reader = new EventStoreManager().OpenReadOnlyStore(file);
        reader.ReadFromStartToEnd(new SkippingEventObserver());
        Assert.True(reader.IsKnownAsFinished());
    }

    [Fact]
    public void EventBiggerThanMaxFileSizeThrows()
    {
        var manager = new EventStoreManager { CompressionStrategy = new NoCompressionStrategy() };
        var appender = manager.AppendToStore(new MemoryEventFileStorage(4096, 4096));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            appender.Store(null, [new Ev { Text = new string('x', 10000) }]));
    }

    // Before the fix an event fitting the maximum file size only without the headers of its later blocks was written
    // past it into a new file, and reading stopped there, so the event was lost.
    [Fact]
    public void EventFittingMaxFileSizeOnlyWithoutBlockHeadersIsStoredOrRefused()
    {
        var stored = 0;
        var refused = 0;
        for (var length = 8100; length < 8200; length++)
        {
            var manager = new EventStoreManager { CompressionStrategy = new NoCompressionStrategy() };
            var appender = manager.AppendToStore(new MemoryEventFileStorage(4096, 8192));
            try
            {
                appender.Store(null, [new Ev { Text = new string('x', length), Num = length }]);
            }
            catch (ArgumentOutOfRangeException)
            {
                refused++;
                continue;
            }

            stored++;
            var reader = new EventStoreManager().OpenReadOnlyStore(appender.CurrentFileStorage);
            var observer = new CollectingEventObserver();
            reader.ReadFromStartToEnd(observer);
            var events = Assert.Single(observer.Events);
            Assert.Equal(length, Assert.IsType<Ev>(Assert.Single(events)).Num);
        }

        Assert.True(stored > 0 && refused > 0, $"{stored} stored, {refused} refused");
    }

    [Fact]
    public void KnownAppendablePositionThrowsWhenNotKnown()
    {
        var file = new MemoryEventFileStorage();
        var manager = new EventStoreManager();
        manager.AppendToStore(file).Store(null, [new Ev()]);
        var appender = new EventStoreManager().AppendToStore(file);
        Assert.Throws<InvalidOperationException>(() => appender.KnownAppendablePosition());
        appender.ReadFromStartToEnd(new SkippingEventObserver());
        Assert.True(appender.KnownAppendablePosition() > 0);
        appender.FinalizeStore();
        Assert.Throws<InvalidOperationException>(() => appender.Store(null, [new Ev()]));
    }

    [Fact]
    public void NoCompressionStrategyCanReadSnappyCompressedStore()
    {
        var strategy = new NoCompressionStrategy();
        Assert.False(strategy.ShouldTryToCompress(100000));
        ReadOnlySpan<byte> data = new byte[100];
        Assert.False(strategy.Compress(ref data));

        var file = new MemoryEventFileStorage();
        new EventStoreManager().AppendToStore(file).Store(null, [new Ev { Text = new string('y', 3000), Num = 3 }]);
        var manager = new EventStoreManager { CompressionStrategy = strategy };
        var observer = new CollectingEventObserver();
        manager.OpenReadOnlyStore(file).ReadFromStartToEnd(observer);
        var ev = Assert.IsType<Ev>(Assert.Single(Assert.Single(observer.Events)));
        Assert.Equal(3000, ev.Text!.Length);
        Assert.Equal(3, ev.Num);
    }

    [Fact]
    public void SkippingEventObserverSkipsEverything()
    {
        var observer = new SkippingEventObserver();
        Assert.False(observer.ObservedMetadata(null!, 1));
        Assert.False(observer.ShouldStopReadingNextEvents());
        observer.ObservedEvents([]);
    }

    [Fact]
    public void EventStoreManagerForgetsTypes()
    {
        var manager = new EventStoreManager();
        var descriptor = manager.DescriptorOf(new Ev());
        Assert.Equal(new FullNameTypeMapper().ToName(typeof(Ev)), descriptor.Name);
        Assert.Same(descriptor, manager.DescriptorOf(new Ev()));
        manager.ForgotAllTypesAndSerializers();
        var descriptor2 = manager.DescriptorOf(new Ev());
        Assert.NotSame(descriptor, descriptor2);
        Assert.True(descriptor.Equals(descriptor2));
        var file = new MemoryEventFileStorage();
        manager.AppendToStore(file).Store(null, [new Ev { Num = 5 }]);
        var observer = new CollectingEventObserver();
        manager.OpenReadOnlyStore(file).ReadFromStartToEnd(observer);
        Assert.Equal(5, Assert.IsType<Ev>(Assert.Single(Assert.Single(observer.Events))).Num);
    }

    // Block checksums catch damage, so reading a store with bytes flipped, inserted, removed or cut off reports a prefix
    // of the stored events and stops.
    [Theory]
    [MemberData(nameof(DamageSeeds))]
    public void DamagedStoreReadsOnlyPrefixOfStoredEvents(int seed)
    {
        var random = new Random(seed);
        var storage = new ArrayEventFileStorage([]);
        var manager = new EventStoreManager { CompressionStrategy = new NoCompressionStrategy() };
        var appender = manager.AppendToStore(storage);
        var stored = new List<int>();
        for (var i = 0; i < 30; i++)
        {
            var count = random.Next(1, 4);
            var events = Enumerable.Range(0, count).Select(j => (object)new Ev { Text = new string('x', random.Next(random.Next(4) == 0 ? 3000 : 20)), Num = i * 10 + j }).ToArray();
            appender.Store(null, events);
            stored.AddRange(events.Select(e => ((Ev)e).Num));
        }

        var original = storage.Data;
        for (var round = 0; round < 30; round++)
        {
            var content = original.ToList();
            for (var changes = random.Next(1, 3); changes > 0; changes--)
            {
                var at = random.Next(content.Count);
                switch (random.Next(4))
                {
                    case 0:
                        content.RemoveRange(at, content.Count - at);
                        break;
                    case 1:
                        content.Insert(at, (byte)random.Next(256));
                        break;
                    case 2:
                        content.RemoveAt(at);
                        break;
                    default:
                        content[at] ^= (byte)(1 << random.Next(8));
                        break;
                }

                if (content.Count == 0) content.Add(0);
            }

            var data = content.ToArray();
            var reader = new EventStoreManager().OpenReadOnlyStore(new ArrayEventFileStorage(data));
            var observer = new CollectingEventObserver();
            try
            {
                reader.ReadFromStartToEnd(observer);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
            }

            var read = observer.Events.SelectMany(e => e).Select(e => ((Ev)e!).Num).ToList();
            Assert.True(read.SequenceEqual(stored.Take(read.Count)), $"Seed {seed} round {round}: read events not stored");
        }
    }

    public static IEnumerable<object[]> DamageSeeds() => Enumerable.Range(1, 200).Select(s => new object[] { s });
}
