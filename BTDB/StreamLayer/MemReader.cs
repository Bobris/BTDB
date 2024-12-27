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

    // This is IMemReader or pinned byte[]
    public object? Controller;

    // Span must be to pinned memory or stack
    unsafe MemReader(ReadOnlySpan<byte> buf)
    {
        Current = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buf));
        Start = Current;
        End = Current + buf.Length;
    }

    public void Dispose()
    {
        if (Controller is IDisposable disposable) disposable.Dispose();
        Start = 0;
        Current = 0;
        End = 0;
        Controller = null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static MemReader CreateFromPinnedSpan(ReadOnlySpan<byte> buf)
    {
        return new(buf);
    }

    public static MemReader CreateFromReadOnlyMemory(ReadOnlyMemory<byte> memory)
    {
        return new(new ReadOnlyMemoryMemReader(memory));
    }

    public static unsafe MemReader CreateFromPinnedArray(byte[] pinnedArray, int offset, int length)
    {
        var start = (nint)Unsafe.AsPointer(ref pinnedArray[offset]);
        return new()
        {
            Current = start,
            Start = start,
            End = start + length,
            Controller = pinnedArray
        };
    }

    // buf is pointer to pinned memory or native memory
    public MemReader(nint buf, nint length)
    {
        Debug.Assert(length >= 0);
        Current = buf;
        Start = buf;
        End = buf + length;
    }

    public unsafe MemReader(void* ptr, int length)
    {
        Debug.Assert(length >= 0);
        Current = (nint)ptr;
        Start = (nint)ptr;
        End = (nint)ptr + length;
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
        controller.Init(ref this);
    }

    // Make sure that buffer is filled by at least length bytes or return true if needs to be BlockRead to continues buffer
    bool FillBuf(nuint length)
    {
        if (Current + (nint)length > End)
        {
            if (Controller is IMemReader memReader)
            {
                memReader.FillBuf(ref this, length);
                return Current + (nint)length > End;
            }

            PackUnpack.ThrowEndOfStreamException();
        }

        return false;
    }

    // Should be called only if Current == End and it will fill buffer by atleast 1 byte or throw EndOfStreamException
    void FillBuf()
    {
        Debug.Assert(Current == End);
        if (Controller is IMemReader memReader)
        {
            memReader.FillBuf(ref this, 1);
            if (Current < End) return;
        }

        PackUnpack.ThrowEndOfStreamException();
    }

    public long GetCurrentPosition()
    {
        return (Controller as IMemReader)?.GetCurrentPosition(this) ?? Current - Start;
    }

    public ulong GetCurrentPositionWithoutController()
    {
        Debug.Assert((Controller as IMemReader)?.ThrowIfNotSimpleReader() ?? true);
        return (ulong)(Current - Start);
    }

    public void SetCurrentPosition(long position)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(position);
        if (Controller is IMemReader memReader)
        {
            memReader.SetCurrentPosition(ref this, position);
        }
        else
        {
            if (Start + position > End) PackUnpack.ThrowEndOfStreamException();
            Current = Start + (nint)position;
        }
    }

    public void SetCurrentPositionWithoutController(ulong position)
    {
        Debug.Assert((Controller as IMemReader)?.ThrowIfNotSimpleReader() ?? true);
        if (position > (nuint)(End - Start)) PackUnpack.ThrowEndOfStreamException();
        Current = Start + (nint)position;
    }

    public bool Eof
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Current == End && ((Controller as IMemReader)?.Eof(ref this) ?? true);
    }

    public unsafe byte ReadUInt8()
    {
        if (Current == End) FillBuf();
        var res = *(byte*)Current;
        Current++;
        return res;
    }

    public unsafe byte PeekUInt8()
    {
        if (Current == End) FillBuf();
        return *(byte*)Current;
    }

    public sbyte ReadInt8Ordered()
    {
        return (sbyte)(ReadUInt8() - 128);
    }

    public void Skip1Byte()
    {
        if (Current == End) FillBuf();
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

    [SkipLocalsInit]
    public unsafe ushort ReadUInt16LE()
    {
        if (FillBuf(2))
        {
            Span<byte> buf = stackalloc byte[2];
            ReadBlock(ref MemoryMarshal.GetReference(buf), 2);
            return PackUnpack.AsLittleEndian(Unsafe.ReadUnaligned<ushort>(ref MemoryMarshal.GetReference(buf)));
        }

        var res = PackUnpack.AsLittleEndian(Unsafe.ReadUnaligned<ushort>((void*)Current));
        Current += 2;
        return res;
    }

    [SkipLocalsInit]
    public unsafe ushort ReadUInt16BE()
    {
        if (FillBuf(2))
        {
            var buf = stackalloc byte[2];
            ReadBlock(buf, 2);
            return PackUnpack.AsBigEndian(Unsafe.ReadUnaligned<ushort>(buf));
        }

        var res = PackUnpack.AsBigEndian(Unsafe.ReadUnaligned<ushort>((void*)Current));
        Current += 2;
        return res;
    }

    public void Skip2Bytes()
    {
        SkipBlock(2);
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

    [SkipLocalsInit]
    public unsafe uint ReadUInt32LE()
    {
        if (FillBuf(4))
        {
            var buf = stackalloc byte[4];
            ReadBlock(buf, 4);
            return PackUnpack.AsLittleEndian(Unsafe.ReadUnaligned<uint>(buf));
        }

        var res = PackUnpack.AsLittleEndian(Unsafe.ReadUnaligned<uint>((void*)Current));
        Current += 4;
        return res;
    }

    [SkipLocalsInit]
    public unsafe uint ReadUInt32BE()
    {
        if (FillBuf(4))
        {
            var buf = stackalloc byte[4];
            ReadBlock(buf, 4);
            return PackUnpack.AsBigEndian(Unsafe.ReadUnaligned<uint>(buf));
        }

        var res = PackUnpack.AsBigEndian(Unsafe.ReadUnaligned<uint>((void*)Current));
        Current += 4;
        return res;
    }

    public void Skip4Bytes()
    {
        SkipBlock(4);
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

    [SkipLocalsInit]
    public unsafe ulong ReadUInt64LE()
    {
        if (FillBuf(8))
        {
            var buf = stackalloc byte[8];
            ReadBlock(buf, 8);
            return PackUnpack.AsLittleEndian(Unsafe.ReadUnaligned<ulong>(buf));
        }

        var res = PackUnpack.AsLittleEndian(Unsafe.ReadUnaligned<ulong>((void*)Current));
        Current += 8;
        return res;
    }

    [SkipLocalsInit]
    public unsafe ulong ReadUInt64BE()
    {
        if (FillBuf(8))
        {
            var buf = stackalloc byte[8];
            ReadBlock(buf, 8);
            return PackUnpack.AsBigEndian(Unsafe.ReadUnaligned<ulong>(buf));
        }

        var res = PackUnpack.AsBigEndian(Unsafe.ReadUnaligned<ulong>((void*)Current));
        Current += 8;
        return res;
    }

    public void Skip8Bytes()
    {
        SkipBlock(8);
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

    public double ReadDoubleOrdered()
    {
        var i = ReadUInt64BE();
        if (i >= 0x8000_0000_0000_0000UL) i -= 0x8000_0000_0000_0000UL;
        else i ^= ulong.MaxValue;
        return BitConverter.UInt64BitsToDouble(i);
    }

    [SkipLocalsInit]
    public unsafe long ReadVInt64()
    {
        if (Current == End) FillBuf();
        var len = PackUnpack.LengthVIntByFirstByte(*(byte*)Current);
        if (FillBuf(len))
        {
            var buf = stackalloc byte[(int)len];
            ReadBlock(buf, len);
            return PackUnpack.UnsafeUnpackVInt(ref Unsafe.AsRef<byte>(buf), len);
        }

        var res = PackUnpack.UnsafeUnpackVInt(ref Unsafe.AsRef<byte>((byte*)Current), len);
        Current += (int)len;
        return res;
    }

    public unsafe void SkipVInt64()
    {
        if (Current == End) FillBuf();
        var len = PackUnpack.LengthVIntByFirstByte(*(byte*)Current);
        SkipBlock(len);
    }

    [SkipLocalsInit]
    public unsafe ulong ReadVUInt64()
    {
        if (Current == End) FillBuf();
        var len = PackUnpack.LengthVUIntByFirstByte(*(byte*)Current);
        if (FillBuf(len))
        {
            var buf = stackalloc byte[(int)len];
            ReadBlock(buf, len);
            return PackUnpack.UnsafeUnpackVUInt(ref Unsafe.AsRef<byte>(buf), len);
        }

        var res = PackUnpack.UnsafeUnpackVUInt(ref Unsafe.AsRef<byte>((byte*)Current), len);
        Current += (int)len;
        return res;
    }

    public unsafe void SkipVUInt64()
    {
        if (Current == End) FillBuf();
        var len = PackUnpack.LengthVUIntByFirstByte(*(byte*)Current);
        SkipBlock(len);
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
        Span<char> charStackBuf = stackalloc char[256];
        var len = 0u;
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

    public ReadOnlySpan<char> ReadStringOrderedAsSpan(scoped ref char charStackBufRef, int charStackBufLength)
    {
        var len = 0u;
        var charBuf = MemoryMarshal.CreateSpan(ref charStackBufRef, charStackBufLength);

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
            if (c == 0) return charBuf[..(int)len];

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
                        return charBuf[..(int)len];
                    }
                }
            }
        }
    }

    [SkipLocalsInit]
    public unsafe string ReadStringInUtf8()
    {
        var len = ReadVUInt64();
        if (len > int.MaxValue) throw new InvalidDataException($"Reading Utf8 String length overflowed with {len}");
        if (len == 0) return "";
        if (FillBuf((nuint)len))
        {
            var buf = len < 2048 ? stackalloc byte[(int)len] : GC.AllocateUninitializedArray<byte>((int)len);
            ReadBlock(buf);
            return Encoding.UTF8.GetString(buf);
        }

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
            if (Controller is IMemReader memReader)
            {
                if (Current < End)
                {
                    Unsafe.CopyBlockUnaligned(ref buffer, ref Unsafe.AsRef<byte>((byte*)Current),
                        (uint)(End - Current));
                    buffer = ref Unsafe.AddByteOffset(ref buffer, End - Current);
                    length -= (uint)(End - Current);
                    Current = End;
                }

                memReader.ReadBlock(ref this, ref buffer, length);
                return;
            }

            Current = End;
            PackUnpack.ThrowEndOfStreamException();
        }

        Unsafe.CopyBlockUnaligned(ref buffer, ref Unsafe.AsRef<byte>((byte*)Current), length);
        Current += (nint)length;
    }

    public unsafe void ReadBlock(byte* buffer, uint length)
    {
        if (length > End - Current)
        {
            if (Controller is IMemReader memReader)
            {
                var len = (uint)(End - Current);
                if (len > 0)
                {
                    Unsafe.CopyBlockUnaligned(buffer, (byte*)Current,
                        len);
                    buffer += len;
                    length -= len;
                    Current = End;
                }

                memReader.ReadBlock(ref this, ref Unsafe.AsRef<byte>(buffer), length);
                return;
            }

            Current = End;
            PackUnpack.ThrowEndOfStreamException();
        }

        Unsafe.CopyBlockUnaligned(buffer, (byte*)Current, length);
        Current += (nint)length;
    }

    public void ReadBlock(in Span<byte> buffer)
    {
        ReadBlock(ref MemoryMarshal.GetReference(buffer), (uint)buffer.Length);
    }

    public unsafe ReadOnlySpan<byte> ReadBlockAsSpan(uint length)
    {
        if (FillBuf(length))
        {
            var buf = GC.AllocateUninitializedArray<byte>((int)length);
            ReadBlock(buf);
            return buf;
        }

        var res = new ReadOnlySpan<byte>((byte*)Current, (int)length);
        Current += (nint)length;
        return res;
    }

    public void SkipBlock(uint length)
    {
        var newCurrent = Current + (nint)length;
        if (newCurrent <= End)
        {
            Current = newCurrent;
            return;
        }

        if (Controller is IMemReader memReader)
        {
            length -= (uint)(End - Current);
            Current = End;

            while (length > 0x4000_0000)
            {
                memReader.SkipBlock(ref this, 0x4000_0000);
                length -= 0x4000_0000;
            }

            memReader.SkipBlock(ref this, length);
            return;
        }

        Current = End;
        PackUnpack.ThrowEndOfStreamException();
    }

    public void SkipByteArray()
    {
        var length = ReadVUInt32();
        if (length == 0) return;
        SkipBlock(length - 1);
    }

    public unsafe ReadOnlySpan<byte> ReadByteArrayAsSpan()
    {
        var length = ReadVUInt32();
        if (length-- <= 1) return new();
        if (FillBuf(length))
        {
            var buf = GC.AllocateUninitializedArray<byte>((int)length);
            ReadBlock(buf);
            return buf;
        }

        var res = new ReadOnlySpan<byte>((byte*)Current, (int)length);
        Current += (nint)length;
        return res;
    }

    [SkipLocalsInit]
    public unsafe Guid ReadGuid()
    {
        if (FillBuf(16))
        {
            var buf = stackalloc byte[16];
            ReadBlock(buf, 16);
            return new(new ReadOnlySpan<byte>(buf, 16));
        }

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

    unsafe ReadOnlyMemory<byte> ReadBlockAsMemory(uint length)
    {
        if (Controller is IMemReader memReader)
        {
            if (memReader.TryReadBlockAsMemory(ref this, length, out var res))
                return res;
        }
        else if (Controller is byte[] pinnedArray)
        {
            var current = Current;
            var newCurrent = current + (nint)length;
            if (newCurrent > End) PackUnpack.ThrowEndOfStreamException();
            Current = newCurrent;
            return MemoryMarshal.CreateFromPinnedArray(pinnedArray,
                (int)(current - (nint)Unsafe.AsPointer(ref pinnedArray[0])), (int)length);
        }

        var resBuffer = GC.AllocateUninitializedArray<byte>((int)length, pinned: true);
        ReadBlock(resBuffer.AsSpan());
        return MemoryMarshal.CreateFromPinnedArray(resBuffer, 0, (int)length);
    }

    public byte[] ReadByteArrayRawTillEof()
    {
        Debug.Assert((Controller as IMemReader)?.ThrowIfNotSimpleReader() ?? true);
        var res = GC.AllocateUninitializedArray<byte>((int)(End - Current));
        ReadBlock(res);
        return res;
    }

    public ReadOnlyMemory<byte> ReadByteArrayRawTillEofAsMemory()
    {
        Debug.Assert((Controller as IMemReader)?.ThrowIfNotSimpleReader() ?? true);
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
            if (Controller is not IMemReader) return false;
            try
            {
                if (FillBuf((uint)magic.Length))
                {
                    Span<byte> buf = stackalloc byte[magic.Length];
                    var pos = GetCurrentPosition();
                    ReadBlock(ref MemoryMarshal.GetReference(buf), (uint)magic.Length);
                    var res = buf.SequenceEqual(magic);
                    if (res == false) SetCurrentPosition(pos);
                    return res;
                }
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
        Debug.Assert((Controller as IMemReader)?.ThrowIfNotSimpleReader() ?? true);
        if (Start + (nint)start + (nint)len > End) throw new ArgumentOutOfRangeException(nameof(len));
        writer.WriteBlock(new ReadOnlySpan<byte>((void*)(Start + (nint)start), (int)len));
    }

    public unsafe void CopyFromPosToWriter(uint start, ref MemWriter writer)
    {
        Debug.Assert((Controller as IMemReader)?.ThrowIfNotSimpleReader() ?? true);
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
        Debug.Assert((Controller as IMemReader)?.ThrowIfNotSimpleReader() ?? true);
        return End - Start;
    }

    public static void InitFromReadOnlyMemory(ref MemReader reader, ReadOnlyMemory<byte> memory)
    {
        if (reader.Controller is ReadOnlyMemoryMemReader readOnlyMemoryMemReader)
        {
            readOnlyMemoryMemReader.Reset(memory);
            readOnlyMemoryMemReader.Init(ref reader);
            return;
        }

        reader.Dispose();
        reader = CreateFromReadOnlyMemory(memory);
    }

    public static unsafe Span<byte> InitFromLen(ref MemReader reader, int size)
    {
        if (reader.Controller is byte[] pinnedArray)
        {
            if (pinnedArray.Length < size)
            {
                reader.Dispose();
                pinnedArray = GC.AllocateUninitializedArray<byte>(size, pinned: true);
                reader = CreateFromPinnedArray(pinnedArray, 0, size);
            }
            else
            {
                reader.Current = reader.Start;
                reader.End = reader.Start + size;
            }

            return new(pinnedArray, 0, size);
        }

        if (reader.Controller == null)
        {
            if (reader.End - reader.Start >= size)
            {
                reader.Current = reader.Start;
                reader.End = reader.Start + size;
                return new((void*)reader.Start, size);
            }
        }

        reader.Dispose();
        if (size == 0) return new();
        var resBuffer = GC.AllocateUninitializedArray<byte>(size, pinned: true);
        reader = CreateFromPinnedArray(resBuffer, 0, size);
        return resBuffer;
    }

    public static void InitFromSpan(ref MemReader reader, ReadOnlySpan<byte> span)
    {
        span.CopyTo(InitFromLen(ref reader, span.Length));
    }

    public readonly unsafe ReadOnlySpan<byte> PeekSpanTillEof()
    {
        return MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((void*)Current), (int)(End - Current));
    }

    public unsafe ReadOnlyMemory<byte> AsReadOnlyMemory()
    {
        Debug.Assert((Controller as IMemReader)?.ThrowIfNotSimpleReader() ?? true);
        var length = (int)(End - Current);
        if (Controller is byte[] pinnedArray)
        {
            return MemoryMarshal.CreateFromPinnedArray(pinnedArray,
                (int)(Current - (nint)Unsafe.AsPointer(ref pinnedArray[0])), (int)(End - Current));
        }

        var resBuffer = GC.AllocateUninitializedArray<byte>(length, pinned: true);
        PeekSpanTillEof().CopyTo(resBuffer.AsSpan());
        return MemoryMarshal.CreateFromPinnedArray(resBuffer, 0, length);
    }

    public nint ReadPointer()
    {
        if (Unsafe.SizeOf<nint>() == 4)
        {
            return ReadInt32LE();
        }
        else
        {
            return (nint)ReadInt64LE();
        }
    }
}
