using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer;

public static class LazyUlongList
{
    internal const int ValuesPerRecord = 4096;
    internal const int MaxRecordPayloadLength = ValuesPerRecord * 9;
    internal const byte CommandClear = 0;
    internal const byte CommandRecord = 1;
    internal const byte CommandCount = 2;

    public static async Task BuildAsync(IEnumerable<ulong> values,
        Func<ReadOnlyMemory<byte>, CancellationToken, Task> applier, Memory<byte> buffer,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(values);
        ArgumentNullException.ThrowIfNull(applier);
        if (buffer.Length < CommandWriter.MaxRecordCommandLength)
            throw new ArgumentException(
                $"Buffer must have at least {CommandWriter.MaxRecordCommandLength} bytes. " +
                "Recommended size is 100 KB.", nameof(buffer));

        var writer = new CommandWriter(buffer, applier, cancellation);
        writer.WriteClear();
        var record = new ulong[ValuesPerRecord];
        var recordIndex = 0ul;
        var count = 0ul;
        var recordCount = 0;
        foreach (var value in values)
        {
            cancellation.ThrowIfCancellationRequested();
            record[recordCount++] = value;
            count = checked(count + 1);
            if (recordCount != ValuesPerRecord)
                continue;
            await writer.WriteRecordAsync(recordIndex++, record, recordCount).ConfigureAwait(false);
            recordCount = 0;
        }

        if (recordCount != 0)
            await writer.WriteRecordAsync(recordIndex, record, recordCount).ConfigureAwait(false);
        writer.WriteCount(count);
        await writer.FlushAsync().ConfigureAwait(false);
    }

    internal static int EncodeRecord(ReadOnlySpan<ulong> values, Span<byte> target)
    {
        if (values is not { Length: > 0 and <= ValuesPerRecord })
            throw new ArgumentException($"A record must contain between 1 and {ValuesPerRecord} values.",
                nameof(values));
        var offset = 0;
        var previous = values[0];
        WriteFirstValue(target, ref offset, previous);
        for (var i = 1; i < values.Length; i++)
        {
            var value = values[i];
            WriteNextValue(target, ref offset, previous, value);
            previous = value;
        }

        return offset;
    }

    internal static int InspectRecord(ReadOnlySpan<byte> source, out ulong lastValue)
    {
        if (source.IsEmpty)
            throw new BTDBException("Lazy ulong list record cannot be empty.");
        var offset = 0;
        var count = 1;
        lastValue = ReadVUInt(source, ref offset);
        while (offset < source.Length)
        {
            if (count == ValuesPerRecord)
                throw new BTDBException($"Lazy ulong list record contains more than {ValuesPerRecord} values.");
            lastValue = unchecked(lastValue + (ulong)ReadVInt(source, ref offset));
            count++;
        }

        return count;
    }

    internal static IEnumerable<ulong> DecodeRecord(ReadOnlyMemory<byte> source)
    {
        if (source.IsEmpty)
            throw new BTDBException("Lazy ulong list record cannot be empty.");
        var reader = BTDB.StreamLayer.MemReader.CreateFromReadOnlyMemory(source);
        try
        {
            var count = 1;
            var previous = reader.ReadVUInt64();
            yield return previous;
            while (!reader.Eof)
            {
                if (count++ == ValuesPerRecord)
                    throw new BTDBException($"Lazy ulong list record contains more than {ValuesPerRecord} values.");
                previous = unchecked(previous + (ulong)reader.ReadVInt64());
                yield return previous;
            }
        }
        finally
        {
            reader.Dispose();
        }
    }

    internal static void WriteFirstValue(Span<byte> target, ref int offset, ulong value)
    {
        var length = PackUnpack.LengthVUInt(value);
        if ((uint)(target.Length - offset) < length)
            throw new ArgumentException("Target is too small.", nameof(target));
        PackUnpack.UnsafePackVUInt(ref target[offset], value, length);
        offset += (int)length;
    }

    internal static void WriteNextValue(Span<byte> target, ref int offset, ulong previous, ulong value)
    {
        // Two's-complement arithmetic keeps every ulong transition representable by one VInt64.
        var difference = unchecked((long)(value - previous));
        var length = PackUnpack.LengthVInt(difference);
        if ((uint)(target.Length - offset) < length)
            throw new ArgumentException("Target is too small.", nameof(target));
        PackUnpack.UnsafePackVInt(ref target[offset], difference, length);
        offset += (int)length;
    }

    static ulong ReadVUInt(ReadOnlySpan<byte> source, ref int offset)
    {
        var length = PackUnpack.LengthVUIntByFirstByte(source[offset]);
        if ((uint)(source.Length - offset) < length)
            throw new BTDBException("Invalid lazy ulong list record.");
        var result = PackUnpack.UnsafeUnpackVUInt(ref Unsafe.AsRef(in source[offset]), length);
        offset += (int)length;
        return result;
    }

    static long ReadVInt(ReadOnlySpan<byte> source, ref int offset)
    {
        var length = PackUnpack.LengthVIntByFirstByte(source[offset]);
        if ((uint)(source.Length - offset) < length)
            throw new BTDBException("Invalid lazy ulong list record.");
        var result = PackUnpack.UnsafeUnpackVInt(ref Unsafe.AsRef(in source[offset]), length);
        offset += (int)length;
        return result;
    }

    sealed class CommandWriter
    {
        const int RecordLengthBytes = sizeof(ushort);
        internal const int MaxRecordCommandLength = 1 + 9 + RecordLengthBytes + MaxRecordPayloadLength;

        readonly Memory<byte> _buffer;
        readonly Func<ReadOnlyMemory<byte>, CancellationToken, Task> _applier;
        readonly CancellationToken _cancellation;
        int _position;

        public CommandWriter(Memory<byte> buffer, Func<ReadOnlyMemory<byte>, CancellationToken, Task> applier,
            CancellationToken cancellation)
        {
            _buffer = buffer;
            _applier = applier;
            _cancellation = cancellation;
        }

        public void WriteClear()
        {
            _buffer.Span[_position++] = CommandClear;
        }

        public async ValueTask WriteRecordAsync(ulong recordIndex, ulong[] values, int count)
        {
            if (_buffer.Length - _position < MaxRecordCommandLength)
                await FlushAsync().ConfigureAwait(false);
            var span = _buffer.Span;
            span[_position++] = CommandRecord;
            WriteVUInt(recordIndex);
            var lengthPosition = _position;
            _position += RecordLengthBytes;
            var payloadLength = EncodeRecord(values.AsSpan(0, count), span[_position..]);
            BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(lengthPosition, RecordLengthBytes),
                (ushort)payloadLength);
            _position += payloadLength;
        }

        public void WriteCount(ulong count)
        {
            if (_buffer.Length - _position < 10)
                FlushAsync().AsTask().GetAwaiter().GetResult();
            _buffer.Span[_position++] = CommandCount;
            WriteVUInt(count);
        }

        public async ValueTask FlushAsync()
        {
            if (_position == 0)
                return;
            await _applier(_buffer[.._position], _cancellation).ConfigureAwait(false);
            _position = 0;
        }

        void WriteVUInt(ulong value)
        {
            var length = PackUnpack.LengthVUInt(value);
            PackUnpack.UnsafePackVUInt(ref _buffer.Span[_position], value, length);
            _position += (int)length;
        }
    }
}
