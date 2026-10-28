using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer.ReadOnly;

/// File format written by <see cref="ReadOnlyCreator"/>. All offsets are from the start of the data and fit 32 bits.
/// <code>
/// Header: "roBTDB" | Int16BE version | VUInt64 key count | VUInt64 commit ulong | VUInt32 ulong count | VUInt64 ulongs
/// Value:  VUInt32 length | bytes (an empty value is stored as offset 0)
/// Leaf:   UInt8 key count (1..127) | VUInt32 prefix length | prefix | UInt16LE end of each key suffix | key suffixes
///         | UInt32LE value offset of each key
/// Branch: UInt8 128 + child count (1..127) | VUInt32 prefix length | prefix | UInt16LE end of each separator suffix
///         | separator suffixes | UInt32LE child offsets | UInt32LE key count in children up to and including this one
/// Footer: UInt32LE root node offset (0 for an empty database)
/// </code>
/// Separator i of a branch is the first key of child i + 1, the prefix is common to the first keys of all children.
static unsafe class ReadOnlyFormat
{
    internal static ReadOnlySpan<byte> Magic => "roBTDB"u8;
    internal const short Version = 1;
    internal const uint MaxChildren = 127;
    internal const uint DefaultMaxChildren = 31;
    internal const int MaxNodeKeyBytes = ushort.MaxValue;
    internal const byte BranchFlag = 128;

    internal static uint ReadUInt16LE(nuint address) =>
        BinaryPrimitives.ReadUInt16LittleEndian(new ReadOnlySpan<byte>((void*)address, 2));

    internal static uint ReadUInt32LE(nuint address) =>
        BinaryPrimitives.ReadUInt32LittleEndian(new ReadOnlySpan<byte>((void*)address, 4));

    internal static ReadOnlySpan<byte> Span(nuint address, uint length) => new((void*)address, (int)length);

    internal static uint ReadVUInt32(nuint address, out uint length)
    {
        ref var first = ref Unsafe.AsRef<byte>((void*)address);
        length = PackUnpack.LengthVUIntByFirstByte(first);
        if (length > 5) throw new InvalidDataException("Corrupted read only database");
        return (uint)PackUnpack.UnsafeUnpackVUInt(ref first, length);
    }
}

/// Immutable key value database stored in one continuous block of memory written by <see cref="ReadOnlyCreator"/>.
/// Usually it is a memory mapped file opened by <see cref="OpenFile"/>. All transactions see the same data and every
/// attempt to modify it throws <see cref="BTDBException"/>.
public sealed class ReadOnlyKeyValueDB : IKeyValueDB
{
    internal readonly nuint Begin;
    internal readonly uint TotalLength;
    internal readonly uint RootOffset;
    internal readonly long KeyValueCount;
    internal readonly ulong CommitUlong;
    internal readonly ulong[] Ulongs;
    MemoryHandle _pinnedMemory;
    MemoryMappedFile? _mappedFile;
    MemoryMappedViewAccessor? _mappedView;
    bool _disposed;

    /// Opens data at the given address. The memory must stay valid and unchanged until this database is disposed.
    public unsafe ReadOnlyKeyValueDB(nuint begin, uint totalLength)
    {
        Begin = begin;
        TotalLength = totalLength;
        if (totalLength < ReadOnlyFormat.Magic.Length + 2 + 4)
            throw new InvalidDataException("Data are too short to be read only database");
        var reader = new MemReader((void*)begin, (int)Math.Min(totalLength - 4, int.MaxValue));
        if (!reader.CheckMagic(ReadOnlyFormat.Magic)) throw new InvalidDataException("Wrong header magic");
        if (reader.ReadInt16BE() != ReadOnlyFormat.Version) throw new InvalidDataException("Unsupported version");
        try
        {
            KeyValueCount = checked((long)reader.ReadVUInt64());
            CommitUlong = reader.ReadVUInt64();
            var ulongCount = reader.ReadVUInt32();
            if (ulongCount > totalLength) throw new InvalidDataException("Corrupted read only database header");
            Ulongs = new ulong[ulongCount];
            for (var i = 0u; i < ulongCount; i++)
            {
                Ulongs[i] = reader.ReadVUInt64();
            }
        }
        // A header running past the data or holding a count no long can, as damage leaves, is truncated data.
        catch (Exception error) when (error is EndOfStreamException or OverflowException)
        {
            throw new InvalidDataException("Corrupted read only database header", error);
        }

        RootOffset = ReadOnlyFormat.ReadUInt32LE(begin + totalLength - 4);
        if (RootOffset >= totalLength - 4 || (RootOffset == 0) != (KeyValueCount == 0))
            throw new InvalidDataException("Corrupted read only database footer");
    }

    /// Opens data in memory, the memory is pinned until this database is disposed and must not change.
    public static unsafe ReadOnlyKeyValueDB Create(ReadOnlyMemory<byte> data)
    {
        var handle = data.Pin();
        try
        {
            return new((nuint)handle.Pointer, (uint)data.Length) { _pinnedMemory = handle };
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    /// Memory maps the file written by <see cref="ReadOnlyCreator.WriteToFile"/>.
    public static unsafe ReadOnlyKeyValueDB OpenFile(string fileName)
    {
        var length = new FileInfo(fileName).Length;
        if (length > uint.MaxValue) throw new InvalidDataException("Read only database cannot be bigger than 4GB");
        var mappedFile = MemoryMappedFile.CreateFromFile(fileName, FileMode.Open, null, 0,
            MemoryMappedFileAccess.Read);
        MemoryMappedViewAccessor? view = null;
        var acquired = false;
        try
        {
            view = mappedFile.CreateViewAccessor(0, length, MemoryMappedFileAccess.Read);
            byte* pointer = null;
            view.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
            acquired = true;
            var begin = (nuint)(pointer + view.PointerOffset);
            return new(begin, (uint)length) { _mappedFile = mappedFile, _mappedView = view };
        }
        catch
        {
            if (acquired) view!.SafeMemoryMappedViewHandle.ReleasePointer();
            view?.Dispose();
            mappedFile.Dispose();
            throw;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _pinnedMemory.Dispose();
        if (_mappedView != null)
        {
            _mappedView.SafeMemoryMappedViewHandle.ReleasePointer();
            _mappedView.Dispose();
            _mappedFile!.Dispose();
        }
    }

    internal void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    internal static BTDBException ReadOnlyException() => new("Read only database cannot be modified");

    public bool DurableTransactions { get; set; }

    public IKeyValueDBTransaction StartTransaction() => StartReadOnlyTransaction();

    public IKeyValueDBTransaction StartReadOnlyTransaction()
    {
        ThrowIfDisposed();
        return new ReadOnlyKeyValueDBTransaction(this);
    }

    public ValueTask<IKeyValueDBTransaction> StartWritingTransaction(bool inBatch = false) =>
        ValueTask.FromException<IKeyValueDBTransaction>(ReadOnlyException());

    public string CalcStats() => $"Read only database with {KeyValueCount} keys in {TotalLength} bytes";

    public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats() =>
        (0, 0, 0, 0);

    public ValueTask<bool> Compact(CancellationToken cancellation) => ValueTask.FromResult(false);

    public void CreateKvi(CancellationToken cancellation) => throw ReadOnlyException();

    public ulong? PreserveHistoryUpToCommitUlong { get; set; }
    public IKeyValueDBLogger? Logger { get; set; }
    public uint CompactorRamLimitInMb { get; set; }
    public long MaxTrLogFileSize { get; set; }

    public IEnumerable<IKeyValueDBTransaction> Transactions() => [];

    public ulong CompactorReadBytesPerSecondLimit { get; set; }
    public ulong CompactorWriteBytesPerSecondLimit { get; set; }
}
