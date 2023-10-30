using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using System.Text;
using BTDB.Buffer;
using Microsoft.Extensions.Primitives;

namespace BTDB.StreamLayer;

public struct MemReader
{
    public nint Current;
    public nint Start;
    public nint End;
    public IMemReader? Controller;

    // Span must be to pinned memory or stack
    public unsafe MemReader(ReadOnlySpan<byte> buf)
    {
        Current = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buf));
        Start = Current;
        End = Current + buf.Length;
    }

    // buf is pointer to pinned memory or native memory
    public MemReader(nint buf, nint length)
    {
        Debug.Assert(length >= 0);
        Current = buf;
        Start = buf;
        End = buf + length;
    }

    public MemReader()
    {
        Current = 0;
        Start = 0;
        End = 0;
    }

    public MemReader(IMemReader controller)
    {
        Controller = controller;
        Controller.Init(ref this);
    }

    void FillBuf(nuint advisePrefetchLength)
    {
        Controller?.FillBuf(ref this, advisePrefetchLength);
        if (Current + (nint)advisePrefetchLength > End) PackUnpack.ThrowEndOfStreamException();
    }

    void FillBuf()
    {
        Controller?.FillBuf(ref this, 1);
        if (Current >= End) PackUnpack.ThrowEndOfStreamException();
    }

    public long GetCurrentPosition()
    {
        return Controller?.GetCurrentPosition(this) ?? Current - Start;
    }

    public uint GetCurrentPositionWithoutController()
    {
        Debug.Assert(Controller?.ThrowIfNotSimpleReader() ?? true);
        return (uint)(Current - Start);
    }

    public void SetCurrentPosition(long position)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(position);
        if (Controller != null)
        {
            Controller.SetCurrentPosition(ref this, position);
        }
        else
        {
            if (position < 0 || Start + position > End) PackUnpack.ThrowEndOfStreamException();
            Current = Start + (nint)position;
        }
    }

    public bool Eof
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (Current == End) && (Controller?.Eof(ref this) ?? true);
    }

    public unsafe byte ReadUInt8()
    {
        if (Current >= End) FillBuf();
        var res = *(byte*)Current;
        Current++;
        return res;
    }

    public unsafe byte PeekUInt8()
    {
        if (Current >= End) FillBuf();
        return *(byte*)Current;
    }

    public sbyte ReadInt8Ordered()
    {
        return (sbyte)(ReadUInt8() - 128);
    }

    public void Skip1Byte()
    {
        if (Current >= End) FillBuf();
        Current++;
    }

    public bool ReadBool()
    {
        return ReadUInt8() != 0;
    }

    public sbyte ReadInt8()
    {
        return (sbyte)ReadUInt8();
    }

    public unsafe ushort ReadUInt16LE()
    {
        if (Current + 2 > End) FillBuf(2);
        var res = PackUnpack.AsLittleEndian(Unsafe.ReadUnaligned<ushort>((void*)Current));
        Current += 2;
        return res;
    }

    public unsafe ushort ReadUInt16BE()
    {
        if (Current + 2 > End) FillBuf(2);
        var res = PackUnpack.AsBigEndian(Unsafe.ReadUnaligned<ushort>((void*)Current));
        Current += 2;
        return res;
    }

    public void Skip2Bytes()
    {
        if (Current + 2 > End) FillBuf(2);
        Current += 2;
    }

    public short ReadInt16LE()
    {
        return (short)ReadUInt16LE();
    }

    public short ReadInt16BE()
    {
        return (short)ReadUInt16BE();
    }

    public Half ReadHalf()
    {
        return BitConverter.UInt16BitsToHalf(ReadUInt16BE());
    }

    public unsafe uint ReadUInt32LE()
    {
        if (Current + 4 > End) FillBuf(4);
        var res = PackUnpack.AsLittleEndian(Unsafe.ReadUnaligned<uint>((void*)Current));
        Current += 4;
        return res;
    }

    public unsafe uint ReadUInt32BE()
    {
        if (Current + 4 > End) FillBuf(4);
        var res = PackUnpack.AsBigEndian(Unsafe.ReadUnaligned<uint>((void*)Current));
        Current += 4;
        return res;
    }

    public void Skip4Bytes()
    {
        if (Current + 4 > End) FillBuf(4);
        Current += 4;
    }

    public int ReadInt32LE()
    {
        return (int)ReadUInt32LE();
    }

    public int ReadInt32BE()
    {
        return (int)ReadUInt32BE();
    }

    public float ReadSingle()
    {
        return BitConverter.UInt32BitsToSingle(ReadUInt32BE());
    }

    public unsafe ulong ReadUInt64LE()
    {
        if (Current + 8 > End) FillBuf(8);
        var res = PackUnpack.AsLittleEndian(Unsafe.ReadUnaligned<ulong>((void*)Current));
        Current += 8;
        return res;
    }

    public unsafe ulong ReadUInt64BE()
    {
        if (Current + 8 > End) FillBuf(8);
        var res = PackUnpack.AsBigEndian(Unsafe.ReadUnaligned<ulong>((void*)Current));
        Current += 8;
        return res;
    }

    public void Skip8Bytes()
    {
        if (Current + 8 > End) FillBuf(8);
        Current += 8;
    }

    public long ReadInt64LE()
    {
        return (long)ReadUInt64LE();
    }

    public long ReadInt64BE()
    {
        return (long)ReadUInt64BE();
    }

    public double ReadDouble()
    {
        return BitConverter.UInt64BitsToDouble(ReadUInt64BE());
    }

    public unsafe long ReadVInt64()
    {
        if (Current >= End) FillBuf();
        var len = PackUnpack.LengthVIntByFirstByte(*(byte*)Current);
        if (Current + len > End) FillBuf(len);
        var res = PackUnpack.UnsafeUnpackVInt(ref Unsafe.AsRef<byte>((byte*)Current), len);
        Current += (int)len;
        return res;
    }

    public unsafe void SkipVInt64()
    {
        if (Current >= End) FillBuf();
        var len = PackUnpack.LengthVIntByFirstByte(*(byte*)Current);
        if (Current + len > End) FillBuf(len);
        Current += (int)len;
    }

    public unsafe ulong ReadVUInt64()
    {
        if (Current >= End) FillBuf();
        var len = PackUnpack.LengthVUIntByFirstByte(*(byte*)Current);
        if (Current + len > End) FillBuf(len);
        var res = PackUnpack.UnsafeUnpackVUInt(ref Unsafe.AsRef<byte>((byte*)Current), len);
        Current += (int)len;
        return res;
    }

    public unsafe void SkipVUInt64()
    {
        if (Current >= End) FillBuf();
        var len = PackUnpack.LengthVUIntByFirstByte(*(byte*)Current);
        if (Current + len > End) FillBuf(len);
        Current += (int)len;
    }

    public short ReadVInt16()
    {
        var res = ReadVInt64();
        if (res is > short.MaxValue or < short.MinValue)
            throw new InvalidDataException(
                $"Reading VInt16 overflowed with {res}");
        return (short)res;
    }

    public void SkipVInt16()
    {
        var res = ReadVInt64();
        if (res is > short.MaxValue or < short.MinValue)
            throw new InvalidDataException(
                $"Skipping VInt16 overflowed with {res}");
    }

    public ushort ReadVUInt16()
    {
        var res = ReadVUInt64();
        if (res > ushort.MaxValue) throw new InvalidDataException($"Reading VUInt16 overflowed with {res}");
        return (ushort)res;
    }

    public void SkipVUInt16()
    {
        var res = ReadVUInt64();
        if (res > ushort.MaxValue) throw new InvalidDataException($"Skipping VUInt16 overflowed with {res}");
    }

    public int ReadVInt32()
    {
        var res = ReadVInt64();
        if (res is > int.MaxValue or < int.MinValue)
            throw new InvalidDataException(
                $"Reading VInt32 overflowed with {res}");
        return (int)res;
    }

    public void SkipVInt32()
    {
        var res = ReadVInt64();
        if (res is > int.MaxValue or < int.MinValue)
            throw new InvalidDataException(
                $"Skipping VInt32 overflowed with {res}");
    }

    public uint ReadVUInt32()
    {
        var res = ReadVUInt64();
        if (res > uint.MaxValue) throw new InvalidDataException($"Reading VUInt32 overflowed with {res}");
        return (uint)res;
    }

    public void SkipVUInt32()
    {
        var res = ReadVUInt64();
        if (res > uint.MaxValue) throw new InvalidDataException($"Skipping VUInt32 overflowed with {res}");
    }

    public DateTime ReadDateTime()
    {
        return DateTime.FromBinary(ReadInt64BE());
    }

    public DateTimeOffset ReadDateTimeOffset()
    {
        var ticks = ReadVInt64();
        var offset = ReadTimeSpan();
        return new(ticks, offset);
    }

    public void SkipDateTimeOffset()
    {
        SkipVInt64();
        SkipVInt64();
    }

    public TimeSpan ReadTimeSpan()
    {
        return new(ReadVInt64());
    }

    public unsafe string? ReadString()
    {
        var len = ReadVUInt64();
        if (len == 0) return null;
        len--;
        if (len > int.MaxValue) throw new InvalidDataException($"Reading String length overflowed with {len}");
        var l = (int)len;
        if (l == 0) return "";
        var result = new string('\0', l);
        fixed (char* res = result)
        {
            var i = 0;
            var least = Math.Min(l, End - Current);
            if ((Sse2.IsSupported || AdvSimd.Arm64.IsSupported) && BitConverter.IsLittleEndian && least >= 32)
            {
                var processed = PackUnpack.WidenAsciiToUtf16Simd(
                    ref Unsafe.AsRef<byte>((byte*)Current), res, (nuint)least);
                i = (int)processed;
                Current += (int)processed;
            }

            while (true)
            {
                while (l - i >= 4 && End - Current >= 4)
                {
                    var v4B = Unsafe.ReadUnaligned<uint>((void*)Current);
                    if (!PackUnpack.AllBytesInUInt32AreAscii(v4B))
                        break;
                    PackUnpack.WidenFourAsciiBytesToUtf16(res + i, v4B);
                    Current += 4;
                    i += 4;
                }

                if (i >= l) break;

                var c = ReadVUInt64();
                if (c > 0xffff)
                {
                    if (c > 0x10ffff)
                        throw new InvalidDataException($"Reading String unicode value overflowed with {c}");
                    c -= 0x10000;
                    res[i++] = (char)((c >> 10) + 0xD800);
                    res[i++] = (char)((c & 0x3FF) + 0xDC00);
                }
                else
                {
                    res[i++] = (char)c;
                }
            }
        }

        return result;
    }

    unsafe ReadOnlySpan<byte> PeekTillEnd()
    {
        return MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((void*)Current),
            (int)Math.Min(int.MaxValue, End - Current));
    }

    [SkipLocalsInit]
    public string? ReadStringOrdered()
    {
        var len = 0u;
        Span<char> charStackBuf = stackalloc char[256];
        var charBuf = charStackBuf;

        var fastSkip = PackUnpack.ReadAndExpandSimpleCharacters(PeekTillEnd(), ref charBuf, ref len);
        if (fastSkip.WasEnd)
        {
            Current += (nint)fastSkip.Count;
            return len == 0 ? "" : new(charBuf[..(int)len]);
        }

        if (fastSkip.Count == 0)
        {
            var c = ReadVUInt32();
            if (c == 0) return "";
            c--;
            if (c == 0x110000) return null;
            if (charBuf.Length < len + 2)
            {
                var newCharBuf = (Span<char>)GC.AllocateUninitializedArray<char>(charBuf.Length * 2);
                charBuf.CopyTo(newCharBuf);
                charBuf = newCharBuf;
            }

            if (c > 0xffff)
            {
                if (c > 0x10ffff)
                {
                    throw new InvalidDataException($"Reading String unicode value overflowed with {c}");
                }

                c -= 0x10000;
                Unsafe.Add(ref MemoryMarshal.GetReference(charBuf), len++) = (char)((c >> 10) + 0xD800);
                Unsafe.Add(ref MemoryMarshal.GetReference(charBuf), len++) = (char)((c & 0x3FF) + 0xDC00);
            }
            else
            {
                Unsafe.Add(ref MemoryMarshal.GetReference(charBuf), len++) = (char)c;
            }
        }
        else
        {
            Current += (nint)fastSkip.Count;
        }

        while (true)
        {
            var c = ReadVUInt32();
            if (c == 0) return new(charBuf[..(int)len]);

            if (charBuf.Length < len + 2)
            {
                var newCharBuf = (Span<char>)new char[charBuf.Length * 2];
                charBuf.CopyTo(newCharBuf);
                charBuf = newCharBuf;
            }

            c--;
            if (c > 0xffff)
            {
                if (c > 0x10ffff)
                {
                    throw new InvalidDataException($"Reading String unicode value overflowed with {c}");
                }

                c -= 0x10000;
                Unsafe.Add(ref MemoryMarshal.GetReference(charBuf), len++) = (char)((c >> 10) + 0xD800);
                Unsafe.Add(ref MemoryMarshal.GetReference(charBuf), len++) = (char)((c & 0x3FF) + 0xDC00);
            }
            else
            {
                Unsafe.Add(ref MemoryMarshal.GetReference(charBuf), len++) = (char)c;
                if (c < 127)
                {
                    var fastSkip2 = PackUnpack.ReadAndExpandSimpleCharacters(PeekTillEnd(), ref charBuf, ref len);
                    Current += (nint)fastSkip2.Count;
                    if (fastSkip2.WasEnd)
                    {
                        return new(charBuf[..(int)len]);
                    }
                }
            }
        }
    }

    public unsafe string ReadStringInUtf8()
    {
        Debug.Assert(Controller?.ThrowIfNotSimpleReader() ?? true);
        var len = ReadVUInt64();
        if (len > int.MaxValue) throw new InvalidDataException($"Reading Utf8 String length overflowed with {len}");
        if (len == 0) return "";
        if (Current + (nint)len > End) FillBuf((nuint)len);
        var res = Encoding.UTF8.GetString((byte*)Current, (int)len);
        Current += (nint)len;
        return res;
    }

    public unsafe void SkipString()
    {
        var len = ReadVUInt64();
        if (len == 0) return;
        len--;
        if (len > int.MaxValue) throw new InvalidDataException($"Skipping String length overflowed with {len}");
        if (len == 0) return;

        var least = Math.Min((nint)len, End - Current);
        if ((Sse2.IsSupported || AdvSimd.Arm64.IsSupported) && BitConverter.IsLittleEndian && least >= 16)
        {
            var processed = PackUnpack.SkipAsciiToUtf16Simd(
                ref Unsafe.AsRef<byte>((byte*)Current), (nuint)least);
            len -= processed;
            Current += (nint)processed;
        }

        while (true)
        {
            while (len >= 4 && End - Current >= 4)
            {
                var v4B = Unsafe.ReadUnaligned<uint>((byte*)Current);
                if (!PackUnpack.AllBytesInUInt32AreAscii(v4B))
                    break;
                Current += 4;
                len -= 4;
            }

            if (len <= 0) break;
            var c = ReadVUInt64();
            if (c > 0xffff)
            {
                if (c > 0x10ffff)
                    throw new InvalidDataException(
                        $"Skipping String unicode value overflowed with {c}");
                len -= 2;
            }
            else
            {
                len--;
            }
        }
    }

    public void SkipStringOrdered()
    {
        var fastSkip = PackUnpack.DetectLengthOfSimpleCharacters(PeekTillEnd());
        if (fastSkip.WasEnd)
        {
            Current += (nint)fastSkip.Count;
            return;
        }

        if (fastSkip.Count == 0)
        {
            var c = ReadVUInt32();
            if (c == 0) return;
            c--;
            if (c > 0x10ffff)
            {
                if (c == 0x110000) return;
                throw new InvalidDataException($"Skipping String unicode value overflowed with {c}");
            }
        }
        else
        {
            Current += (nint)fastSkip.Count;
        }

        while (true)
        {
            var c = ReadVUInt32();
            if (c == 0) break;
            c--;
            if (c > 0x10ffff) throw new InvalidDataException($"Skipping String unicode value overflowed with {c}");
            if (c < 127)
            {
                var fastSkip2 = PackUnpack.DetectLengthOfSimpleCharacters(PeekTillEnd());
                Current += (nint)fastSkip2.Count;
                if (fastSkip2.WasEnd)
                {
                    return;
                }
            }
        }
    }

    public void SkipStringInUtf8()
    {
        var len = ReadVUInt64();
        if (len > int.MaxValue)
            throw new InvalidDataException($"Skipping Utf8 String length overflowed with {len}");
        SkipBlock((uint)len);
    }

    public unsafe void ReadBlock(ref byte buffer, uint length)
    {
        if (length > End - Current)
        {
            if (Controller != null)
            {
                if (Current < End)
                {
                    Unsafe.CopyBlockUnaligned(ref buffer, ref Unsafe.AsRef<byte>((byte*)Current),
                        (uint)(End - Current));
                    buffer = ref Unsafe.AddByteOffset(ref buffer, End - Current);
                    length -= (uint)(End - Current);
                    Current = End;
                }

                Controller.ReadBlock(ref this, ref buffer, length);
                return;
            }

            Current = End;
            PackUnpack.ThrowEndOfStreamException();
        }

        if (Current + (nint)length > End) FillBuf(length);
        Unsafe.CopyBlockUnaligned(ref buffer, ref Unsafe.AsRef<byte>((byte*)Current), length);
        Current += (nint)length;
    }

    public void ReadBlock(in Span<byte> buffer)
    {
        ReadBlock(ref MemoryMarshal.GetReference(buffer), (uint)buffer.Length);
    }

    public unsafe ReadOnlySpan<byte> ReadBlockAsSpan(uint length)
    {
        Debug.Assert(Controller?.ThrowIfNotSimpleReader() ?? true);
        if (Current + length > End) FillBuf(length);
        var res = new ReadOnlySpan<byte>((byte*)Current, (int)length);
        Current += (nint)length;
        return res;
    }

    public void SkipBlock(uint length)
    {
        if (length > End - Current)
        {
            if (Controller != null)
            {
                length -= (uint)(End - Current);
                Current = End;

                while (length > (uint)int.MaxValue + 1)
                {
                    Controller.SkipBlock(ref this, (uint)int.MaxValue + 1);
                    length -= (uint)int.MaxValue + 1;
                }

                Controller.SkipBlock(ref this, length);
                return;
            }

            Current = End;
            PackUnpack.ThrowEndOfStreamException();
        }

        Current += (nint)length;
    }

    public void SkipByteArray()
    {
        var length = ReadVUInt32();
        if (length == 0) return;
        SkipBlock(length - 1);
    }

    public unsafe ReadOnlySpan<byte> ReadByteArrayAsSpan()
    {
        Debug.Assert(Controller?.ThrowIfNotSimpleReader() ?? true);
        var length = ReadVUInt32();
        if (length-- <= 1) return new();
        if (Current + length > End) FillBuf(length);
        var res = new ReadOnlySpan<byte>((byte*)Current, (int)length);
        Current += (nint)length;
        return res;
    }

    public unsafe Guid ReadGuid()
    {
        if (Current + 16 > End) FillBuf(16);
        var res = new Guid(new ReadOnlySpan<byte>((byte*)Current, 16));
        Current += 16;
        return res;
    }

    public void SkipGuid()
    {
        SkipBlock(16);
    }

    public decimal ReadDecimal()
    {
        var header = ReadUInt8();
        ulong first = 0;
        uint second = 0;
        switch (header >> 5 & 3)
        {
            case 0:
                break;
            case 1:
                first = ReadVUInt64();
                break;
            case 2:
                second = ReadVUInt32();
                first = (ulong)ReadInt64BE();
                break;
            case 3:
                second = (uint)ReadInt32BE();
                first = (ulong)ReadInt64BE();
                break;
        }

        var res = new decimal((int)first, (int)(first >> 32), (int)second, (header & 128) != 0,
            (byte)(header & 31));
        return res;
    }

    public void SkipDecimal()
    {
        var header = ReadUInt8();
        switch (header >> 5 & 3)
        {
            case 0:
                break;
            case 1:
                SkipVUInt64();
                break;
            case 2:
                SkipVUInt32();
                Skip8Bytes();
                break;
            case 3:
                SkipBlock(12);
                break;
        }
    }

    public byte[]? ReadByteArray()
    {
        var length = ReadVUInt32();
        if (length == 0) return null;
        var bytes = GC.AllocateUninitializedArray<byte>((int)length - 1);
        ReadBlock(bytes);
        return bytes;
    }

    public ReadOnlyMemory<byte> ReadByteArrayAsMemory()
    {
        var length = ReadVUInt32();
        if (length-- <= 1) return new();
        return ReadBlockAsMemory(length);
    }

    ReadOnlyMemory<byte> ReadBlockAsMemory(uint length)
    {
        if (Controller != null && Controller.TryReadBlockAsMemory(ref this, length, out var res))
        {
            return res;
        }

        var resBuffer = GC.AllocateUninitializedArray<byte>((int)length, pinned: true);
        ReadBlock(resBuffer.AsSpan());
        return MemoryMarshal.CreateFromPinnedArray(resBuffer, 0, (int)length);
    }

    public byte[] ReadByteArrayRawTillEof()
    {
        Debug.Assert(Controller?.ThrowIfNotSimpleReader() ?? true);
        var res = GC.AllocateUninitializedArray<byte>((int)(End - Current));
        ReadBlock(res);
        return res;
    }

    public ReadOnlyMemory<byte> ReadByteArrayRawTillEofAsMemory()
    {
        Debug.Assert(Controller?.ThrowIfNotSimpleReader() ?? true);
        return ReadBlockAsMemory((uint)(End - Current));
    }

    public byte[] ReadByteArrayRaw(int len)
    {
        var res = GC.AllocateUninitializedArray<byte>(len);
        ReadBlock(res);
        return res;
    }

    [SkipLocalsInit]
    public unsafe bool CheckMagic(ReadOnlySpan<byte> magic)
    {
        if (End - Current < magic.Length)
        {
            if (Controller == null) return false;

            try
            {
                FillBuf((uint)magic.Length);
            }
            catch (EndOfStreamException)
            {
                return false;
            }
        }

        if (!new ReadOnlySpan<byte>((void*)Current, magic.Length).SequenceEqual(magic)) return false;
        Current += magic.Length;
        return true;
    }

    [SkipLocalsInit]
    public IPAddress? ReadIPAddress()
    {
        switch (ReadUInt8())
        {
            case 0:
                return new((uint)ReadInt32LE());
            case 1:
            {
                Span<byte> ip6Bytes = stackalloc byte[16];
                ReadBlock(ref MemoryMarshal.GetReference(ip6Bytes), 16);
                return new(ip6Bytes);
            }
            case 2:
            {
                Span<byte> ip6Bytes = stackalloc byte[16];
                ReadBlock(ref MemoryMarshal.GetReference(ip6Bytes), 16);
                var scopeId = (long)ReadVUInt64();
                return new(ip6Bytes, scopeId);
            }
            case 3:
                return null;
            default: throw new InvalidDataException("Unknown type of IPAddress");
        }
    }

    public void SkipIPAddress()
    {
        switch (ReadUInt8())
        {
            case 0:
                Skip4Bytes();
                return;
            case 1:
                SkipBlock(16);
                return;
            case 2:
                SkipBlock(16);
                SkipVUInt64();
                return;
            case 3:
                return;
            default: throw new InvalidDataException("Unknown type of IPAddress");
        }
    }

    public Version? ReadVersion()
    {
        var major = ReadVUInt32();
        if (major == 0) return null;
        var minor = ReadVUInt32();
        if (minor == 0) return new((int)major - 1, 0);
        var build = ReadVUInt32();
        if (build == 0) return new((int)major - 1, (int)minor - 1);
        var revision = ReadVUInt32();
        if (revision == 0) return new((int)major - 1, (int)minor - 1, (int)build - 1);
        return new((int)major - 1, (int)minor - 1, (int)build - 1, (int)revision - 1);
    }

    public void SkipVersion()
    {
        var major = ReadVUInt32();
        if (major == 0) return;
        var minor = ReadVUInt32();
        if (minor == 0) return;
        var build = ReadVUInt32();
        if (build == 0) return;
        SkipVUInt32();
    }

    public StringValues ReadStringValues()
    {
        var count = ReadVUInt32();
        var a = new string[count];
        for (var i = 0u; i < count; i++)
        {
            a[i] = ReadString();
        }

        return new(a);
    }

    public void SkipStringValues()
    {
        var count = ReadVUInt32();
        for (var i = 0u; i < count; i++)
        {
            SkipString();
        }
    }

    public unsafe void CopyAbsoluteToWriter(uint start, uint len, ref MemWriter writer)
    {
        Debug.Assert(Controller?.ThrowIfNotSimpleReader() ?? true);
        writer.WriteBlock(new ReadOnlySpan<byte>((void*)(Start + (nint)start), (int)len));
    }

    public unsafe void CopyFromPosToWriter(uint start, ref MemWriter writer)
    {
        Debug.Assert(Controller?.ThrowIfNotSimpleReader() ?? true);
        writer.WriteBlock(new ReadOnlySpan<byte>((void*)(Start + (nint)start), (int)(Current - Start - start)));
    }

    public override unsafe string ToString()
    {
        var length = (nuint)(End - Start);
        var pos = (nuint)(Current - Start);
        if (length < 100)
        {
            return Convert.ToHexString(MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>((byte*)Start), (int)pos)) + "|" +
                   Convert.ToHexString(
                       MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>((byte*)Start), (int)length)[(int)pos..]);
        }

        return (pos < 50
                   ? Convert.ToHexString(MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>((byte*)Start), (int)pos))
                   : Convert.ToHexString(MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>((byte*)Start), 20)) + "..." +
                     Convert.ToHexString(MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>((byte*)Start), (int)length)
                         .Slice((int)pos - 20, 20))) +
               "|" +
               Convert.ToHexString(MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>((byte*)Start), (int)length)
                   .Slice((int)pos, int.Min((int)(length - pos), 50)));
    }

    public void UnreadByte()
    {
        if (Current == Start)
            SetCurrentPosition(GetCurrentPosition() - 1);
        else
            Current--;
    }

    public nint GetLength()
    {
        Debug.Assert(Controller?.ThrowIfNotSimpleReader() ?? true);
        return End - Start;
    }
}
