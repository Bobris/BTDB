using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer.ReadOnly;

public class ReadOnlyKeyValueDB : IKeyValueDB
{
    internal readonly nuint _begin;
    internal readonly uint _totalLen;
    internal readonly uint _rootNodeOffset;
    internal readonly uint _keyValueCount;
    internal readonly ulong _commitUlong;
    internal readonly ulong[] _ulongs;

    public ReadOnlyKeyValueDB(nuint begin, uint totalLen)
    {
        _begin = begin;
        _totalLen = totalLen;
        var reader = new SpanReader(BitArrayManipulation.CreateReadOnlySpan(_begin, (int)long.Min(_totalLen, int.MaxValue)));
        if (!reader.CheckMagic("roBTDB"u8)) throw new InvalidDataException("Wrong header magic");
        if (reader.ReadInt16() != 1) throw new InvalidDataException("Unsupported version");
        _keyValueCount = reader.ReadVUInt32();
        _commitUlong = reader.ReadVUInt64();
        var ulongCount = reader.ReadVUInt32();
        _ulongs = new ulong[ulongCount];
        for (var i = 0u; i < ulongCount; i++)
        {
            _ulongs[i] = reader.ReadVUInt64();
        }
        _rootNodeOffset = BinaryPrimitives.ReadUInt32LittleEndian(BitArrayManipulation.CreateReadOnlySpan(begin + totalLen - 4, 4));
    }

    public void Dispose()
    {
    }

    public bool DurableTransactions { get; set; }

    public IKeyValueDBTransaction StartTransaction()
    {
        return StartReadOnlyTransaction();
    }

    public IKeyValueDBTransaction StartReadOnlyTransaction()
    {
        return new ReadOnlyKeyValueDBTransaction(this);
    }

    public ValueTask<IKeyValueDBTransaction> StartWritingTransaction()
    {
        throw new System.NotSupportedException("It is readonly db");
    }

    public string CalcStats()
    {
        return "";
    }

    public (ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) GetNativeMemoryStats()
    {
        return (_totalLen, 1, 0, 0);
    }

    public bool Compact(CancellationToken cancellation)
    {
        return false;
    }

    public void CreateKvi(CancellationToken cancellation)
    {
        throw new System.NotSupportedException();
    }

    public ulong? PreserveHistoryUpToCommitUlong { get; set; }
    public IKeyValueDBLogger? Logger { get; set; }
    public uint CompactorRamLimitInMb { get; set; }
    public long MaxTrLogFileSize { get; set; }
    public IEnumerable<IKeyValueDBTransaction> Transactions()
    {
        yield break;
    }

    public ulong CompactorReadBytesPerSecondLimit { get; set; }
    public ulong CompactorWriteBytesPerSecondLimit { get; set; }
}
