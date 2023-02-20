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

public ref struct SpanReader
{
    public SpanReader(ReadOnlySpan<byte> data)
    {
        Buf = data;
        Original = data;
        Controller = null;
    }

    public SpanReader(ReadOnlyMemory<byte> data)
    {
        OriginalMemory = data;
        Buf = data.Span;
        Original = Buf;
        Controller = null;
    }

    public SpanReader(byte[] data)
    {
        OriginalMemory = data;
        Buf = data.AsSpan();
        Original = Buf;
        Controller = null;
    }

    public SpanReader(in ByteBuffer data)
    {
        OriginalMemory = data.AsSyncReadOnlyMemory();
        Buf = OriginalMemory.Span;
        Original = Buf;
        Controller = null;
    }

    public SpanReader(ISpanReader controller)
    {
        Controller = controller;
        Buf = new();
        Original = new();
        controller.Init(ref this);
    }

    public ReadOnlySpan<byte> Buf;
    public ReadOnlySpan<byte> Original;
    public ReadOnlyMemory<byte> OriginalMemory;
    public readonly ISpanReader? Controller;

    /// <summary>
    /// Remembers actual position into controller. Useful for continuing reading across async calls.
    /// This method could be called only once and only as last called method on SpanReader instance.
    /// </summary>
    public void Sync()
    {
        if (Controller == null) ThrowCanBeUsedOnlyWithController();
        Controller!.Sync(ref this);
    }

    static void ThrowCanBeUsedOnlyWithController()
    {
        throw new InvalidOperationException("Need controller");
    }

    public long GetCurrentPosition()
    {
        return Controller?.GetCurrentPosition(this) ?? (long)Unsafe.ByteOffset(
            ref MemoryMarshal.GetReference(Original),
            ref MemoryMarshal.GetReference(Buf));
    }

    public uint GetCurrentPositionWithoutController()
    {
        Debug.Assert(Controller == null);
        return (uint)Unsafe.ByteOffset(
            ref MemoryMarshal.GetReference(Original),
            ref MemoryMarshal.GetReference(Buf));
    }

    public void SetCurrentPosition(long position)
    {
        if (Controller != null)
        {
            Controller.SetCurrentPosition(ref this, position);
        }
        else
        {
            Buf = Original.Slice((int)position);
        }
    }

    public bool Eof
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Buf.IsEmpty && (Controller?.FillBufAndCheckForEof(ref this) ?? true);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void NeedOneByteInBuffer()
    {
        if (Eof)
        {
            PackUnpack.ThrowEndOfStreamException();
        }
    }

    ref byte PessimisticBlockReadAsByteRef(ref byte buf, uint len)
    {
        if (Controller == null)
        {
            if ((uint)Buf.Length >= len)
            {
                ref var res = ref MemoryMarshal.GetReference(Buf);
                PackUnpack.UnsafeAdvance(ref Buf, (int)len);
                return ref res;
            }

            PackUnpack.ThrowEndOfStreamException();
        }
        else
        {
            if (Controller.FillBufAndCheckForEof(ref this))
                PackUnpack.ThrowEndOfStreamException();
            if ((uint)Buf.Length >= len)
            {
                ref var res = ref MemoryMarshal.GetReference(Buf);
                PackUnpack.UnsafeAdvance(ref Buf, (int)len);
                return ref res;
            }

            ref var cur = ref buf;
            do
            {
                Unsafe.CopyBlockUnaligned(ref cur, ref MemoryMarshal.GetReference(Buf), (uint)Buf.Length);
                cur = ref Unsafe.AddByteOffset(ref cur, (IntPtr)Buf.Length);
                len -= (uint)Buf.Length;
                Buf = new();
                if (Controller.FillBufAndCheckForEof(ref this))
                    PackUnpack.ThrowEndOfStreamException();
            } while (len > (uint)Buf.Length);

            Unsafe.CopyBlockUnaligned(ref cur, ref MemoryMarshal.GetReference(Buf), len);
            PackUnpack.UnsafeAdvance(ref Buf, (int)len);
        }

        return ref buf;
    }

    public bool ReadBool()
    {
        NeedOneByteInBuffer();
        return PackUnpack.UnsafeGetAndAdvance(ref Buf, 1) != 0;
    }

    public void SkipBool()
    {
        SkipUInt8();
    }

    public byte ReadUInt8()
    {
        NeedOneByteInBuffer();
        return PackUnpack.UnsafeGetAndAdvance(ref Buf, 1);
    }

    public void SkipUInt8()
    {
        NeedOneByteInBuffer();
        PackUnpack.UnsafeAdvance(ref Buf, 1);
    }

    public sbyte ReadInt8()
    {
        NeedOneByteInBuffer();
        return (sbyte)PackUnpack.UnsafeGetAndAdvance(ref Buf, 1);
    }

    public void SkipInt8()
    {
        SkipUInt8();
    }

    public sbyte ReadInt8Ordered()
    {
        NeedOneByteInBuffer();
        return (sbyte)(PackUnpack.UnsafeGetAndAdvance(ref Buf, 1) - 128);
    }

    public void SkipInt8Ordered()
    {
        SkipUInt8();
    }

    public short ReadVInt16()
    {
        var res = ReadVInt64();
        if (res > short.MaxValue || res < short.MinValue)
            throw new InvalidDataException(
                $"Reading VInt16 overflowed with {res}");
        return (short)res;
    }

    public void SkipVInt16()
    {
        var res = ReadVInt64();
        if (res > short.MaxValue || res < short.MinValue)
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
        if (res > int.MaxValue || res < int.MinValue)
            throw new InvalidDataException(
                $"Reading VInt32 overflowed with {res}");
        return (int)res;
    }

    public void SkipVInt32()
    {
        var res = ReadVInt64();
        if (res > int.MaxValue || res < int.MinValue)
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

    [SkipLocalsInit]
    public long ReadVInt64()
    {
        NeedOneByteInBuffer();
        ref var byteRef = ref MemoryMarshal.GetReference(Buf);
        var len = PackUnpack.LengthVIntByFirstByte(byteRef);
        if (len <= (uint)Buf.Length)
        {
            PackUnpack.UnsafeAdvance(ref Buf, (int)len);
            return PackUnpack.UnsafeUnpackVInt(ref byteRef, len);
        }
        else
        {
            Span<byte> buf = stackalloc byte[(int)len];
            return PackUnpack.UnsafeUnpackVInt(
                ref PessimisticBlockReadAsByteRef(ref MemoryMarshal.GetReference(buf), len), len);
        }
    }

    public void SkipVInt64()
    {
        NeedOneByteInBuffer();
        var len = PackUnpack.LengthVIntByFirstByte(MemoryMarshal.GetReference(Buf));
        if (len <= (uint)Buf.Length)
        {
            PackUnpack.UnsafeAdvance(ref Buf, (int)len);
        }
        else
        {
            SkipBlock(len);
        }
    }

    [SkipLocalsInit]
    public ulong ReadVUInt64()
    {
        NeedOneByteInBuffer();
        ref var byteRef = ref MemoryMarshal.GetReference(Buf);
        var len = PackUnpack.LengthVUIntByFirstByte(byteRef);
        if (len <= (uint)Buf.Length)
        {
            PackUnpack.UnsafeAdvance(ref Buf, (int)len);
            return PackUnpack.UnsafeUnpackVUInt(ref byteRef, len);
        }
        else
        {
            Span<byte> buf = stackalloc byte[(int)len];
            return PackUnpack.UnsafeUnpackVUInt(
                ref PessimisticBlockReadAsByteRef(ref MemoryMarshal.GetReference(buf), len), len);
        }
    }

    public void SkipVUInt64()
    {
        NeedOneByteInBuffer();
        var len = PackUnpack.LengthVUIntByFirstByte(MemoryMarshal.GetReference(Buf));
        if (len <= (uint)Buf.Length)
        {
            PackUnpack.UnsafeAdvance(ref Buf, (int)len);
        }
        else
        {
            SkipBlock(len);
        }
    }

    [SkipLocalsInit]
    public long ReadInt64()
    {
        if (8 <= (uint)Buf.Length)
        {
            return (long)PackUnpack.AsBigEndian(
                Unsafe.As<byte, ulong>(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, 8)));
        }
        else
        {
            Span<byte> buf = stackalloc byte[8];
            return (long)PackUnpack.AsBigEndian(
                Unsafe.As<byte, ulong>(ref PessimisticBlockReadAsByteRef(ref MemoryMarshal.GetReference(buf), 8)));
        }
    }

    public void SkipInt64()
    {
        if (8 <= (uint)Buf.Length)
        {
            PackUnpack.UnsafeAdvance(ref Buf, 8);
        }
        else
        {
            SkipBlock(8);
        }
    }

    [SkipLocalsInit]
    public int ReadInt32()
    {
        if (4 <= (uint)Buf.Length)
        {
            return (int)PackUnpack.AsBigEndian(
                Unsafe.As<byte, uint>(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, 4)));
        }
        else
        {
            Span<byte> buf = stackalloc byte[4];
            return (int)PackUnpack.AsBigEndian(
                Unsafe.As<byte, uint>(ref PessimisticBlockReadAsByteRef(ref MemoryMarshal.GetReference(buf), 4)));
        }
    }

    [SkipLocalsInit]
    public short ReadInt16()
    {
        if (2 <= (uint)Buf.Length)
        {
            return (short)PackUnpack.AsBigEndian(
                Unsafe.As<byte, ushort>(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, 2)));
        }
        else
        {
            Span<byte> buf = stackalloc byte[2];
            return (short)PackUnpack.AsBigEndian(
                Unsafe.As<byte, ushort>(ref PessimisticBlockReadAsByteRef(ref MemoryMarshal.GetReference(buf), 2)));
        }
    }

    [SkipLocalsInit]
    public int ReadInt32LE()
    {
        if (4 <= (uint)Buf.Length)
        {
            return (int)PackUnpack.AsLittleEndian(
                Unsafe.As<byte, uint>(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, 4)));
        }
        else
        {
            Span<byte> buf = stackalloc byte[4];
            return (int)PackUnpack.AsLittleEndian(
                Unsafe.As<byte, uint>(ref PessimisticBlockReadAsByteRef(ref MemoryMarshal.GetReference(buf), 4)));
        }
    }

    public void SkipInt32()
    {
        if (4 <= (uint)Buf.Length)
        {
            PackUnpack.UnsafeAdvance(ref Buf, 4);
        }
        else
        {
            SkipBlock(4);
        }
    }

    public void SkipInt16()
    {
        if (2 <= (uint)Buf.Length)
        {
            PackUnpack.UnsafeAdvance(ref Buf, 2);
        }
        else
        {
            SkipBlock(2);
        }
    }

    public DateTime ReadDateTime()
    {
        return DateTime.FromBinary(ReadInt64());
    }

    public void SkipDateTime()
    {
        SkipInt64();
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
        SkipTimeSpan();
    }

    public TimeSpan ReadTimeSpan()
    {
        return new(ReadVInt64());
    }

    public void SkipTimeSpan()
    {
        SkipVInt64();
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
            var least = Math.Min(l, Buf.Length);
            if ((Sse2.IsSupported || AdvSimd.Arm64.IsSupported) && BitConverter.IsLittleEndian && least >= 32)
            {
                var processed = PackUnpack.WidenAsciiToUtf16Simd(
                    (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(Buf)), res, (nuint)least);
                i = (int)processed;
                PackUnpack.UnsafeAdvance(ref Buf, (int)processed);
            }

            while (true)
            {
                while (l - i >= 4 && Buf.Length >= 4)
                {
                    var v4b = PackUnpack.UnsafeGet<uint>(Buf);
                    if (!PackUnpack.AllBytesInUInt32AreAscii(v4b))
                        break;
                    PackUnpack.WidenFourAsciiBytesToUtf16(res + i, v4b);
                    PackUnpack.UnsafeAdvance(ref Buf, 4);
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

    public ReadOnlySpan<char> ReadStringOrderedAsSpan(ref char buffer, int bufferLength)
    {
        var len = 0;
        var charBuf = MemoryMarshal.CreateSpan(ref buffer, bufferLength);

        while (true)
        {
            var c = ReadVUInt32();
            if (c == 0) break;
            c--;
            if (c > 0xffff)
            {
                if (c > 0x10ffff)
                {
                    if (len == 0 && c == 0x110000) return null;
                    throw new InvalidDataException($"Reading String unicode value overflowed with {c}");
                }

                c -= 0x10000;
                if (charBuf.Length < len + 2)
                {
                    var newCharBuf = (Span<char>)new char[charBuf.Length * 2];
                    charBuf.CopyTo(newCharBuf);
                    charBuf = newCharBuf;
                }

                Unsafe.Add(ref MemoryMarshal.GetReference(charBuf), len++) = (char)((c >> 10) + 0xD800);
                Unsafe.Add(ref MemoryMarshal.GetReference(charBuf), len++) = (char)((c & 0x3FF) + 0xDC00);
            }
            else
            {
                if (charBuf.Length < len + 1)
                {
                    var newCharBuf = (Span<char>)new char[charBuf.Length * 2];
                    charBuf.CopyTo(newCharBuf);
                    charBuf = newCharBuf;
                }

                Unsafe.Add(ref MemoryMarshal.GetReference(charBuf), len++) = (char)c;
            }
        }

        return charBuf[..len];
    }

    [SkipLocalsInit]
    public string? ReadStringOrdered()
    {
        var len = 0u;
        Span<char> charStackBuf = stackalloc char[256];
        var charBuf = charStackBuf;

        var fastSkip = PackUnpack.ReadAndExpandSimpleCharacters(Buf, ref charBuf, ref len);
        if (fastSkip.WasEnd)
        {
            PackUnpack.UnsafeAdvance(ref Buf, (int)fastSkip.Count);
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
                var newCharBuf = (Span<char>)new char[charBuf.Length * 2];
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
            PackUnpack.UnsafeAdvance(ref Buf, (int)fastSkip.Count);
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
                    var fastSkip2 = PackUnpack.ReadAndExpandSimpleCharacters(Buf, ref charBuf, ref len);
                    PackUnpack.UnsafeAdvance(ref Buf, (int)fastSkip2.Count);
                    if (fastSkip2.WasEnd)
                    {
                        return new(charBuf[..(int)len]);
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
        var l = (int)len;
        if (l == 0) return "";
        if (l <= (uint)Buf.Length)
        {
            return Encoding.UTF8.GetString((byte*)Unsafe.AsPointer(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, l)),
                l);
        }

        Span<byte> buf = l <= 512 ? stackalloc byte[l] : new byte[l];
        return Encoding.UTF8.GetString(
            (byte*)Unsafe.AsPointer(
                ref PessimisticBlockReadAsByteRef(ref MemoryMarshal.GetReference(buf), (uint)l)), l);
    }

    public unsafe void SkipString()
    {
        var len = ReadVUInt64();
        if (len == 0) return;
        len--;
        if (len > int.MaxValue) throw new InvalidDataException($"Skipping String length overflowed with {len}");
        var l = (int)len;
        if (l == 0) return;

        var least = Math.Min(l, Buf.Length);
        if ((Sse2.IsSupported || AdvSimd.Arm64.IsSupported) && BitConverter.IsLittleEndian && least >= 16)
        {
            var processed = PackUnpack.SkipAsciiToUtf16Simd(
                (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(Buf)), (nuint)least);
            l -= (int)processed;
            PackUnpack.UnsafeAdvance(ref Buf, (int)processed);
        }

        while (true)
        {
            while (l >= 4 && Buf.Length >= 4)
            {
                var v4b = PackUnpack.UnsafeGet<uint>(Buf);
                if (!PackUnpack.AllBytesInUInt32AreAscii(v4b))
                    break;
                PackUnpack.UnsafeAdvance(ref Buf, 4);
                l -= 4;
            }

            if (l <= 0) break;
            var c = ReadVUInt64();
            if (c > 0xffff)
            {
                if (c > 0x10ffff)
                    throw new InvalidDataException(
                        $"Skipping String unicode value overflowed with {c}");
                l -= 2;
            }
            else
            {
                l--;
            }
        }
    }

    public void SkipStringOrdered()
    {
        var fastSkip = PackUnpack.DetectLengthOfSimpleCharacters(Buf);
        if (fastSkip.WasEnd)
        {
            PackUnpack.UnsafeAdvance(ref Buf, (int)fastSkip.Count);
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
            PackUnpack.UnsafeAdvance(ref Buf, (int)fastSkip.Count);
        }

        while (true)
        {
            var c = ReadVUInt32();
            if (c == 0) break;
            c--;
            if (c > 0x10ffff) throw new InvalidDataException($"Skipping String unicode value overflowed with {c}");
            if (c < 127)
            {
                var fastSkip2 = PackUnpack.DetectLengthOfSimpleCharacters(Buf);
                PackUnpack.UnsafeAdvance(ref Buf, (int)fastSkip2.Count);
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

    public void ReadBlock(ref byte buffer, uint length)
    {
        if (length > Buf.Length)
        {
            if (Controller != null)
            {
                if (Buf.Length > 0)
                {
                    Unsafe.CopyBlockUnaligned(ref buffer, ref MemoryMarshal.GetReference(Buf), (uint)Buf.Length);
                    buffer = ref Unsafe.AddByteOffset(ref buffer, (IntPtr)Buf.Length);
                    length -= (uint)Buf.Length;
                    Buf = new();
                }

                if (!Controller.ReadBlock(ref this, ref buffer, length)) return;
            }

            Buf = new();
            PackUnpack.ThrowEndOfStreamException();
        }

        Unsafe.CopyBlockUnaligned(ref buffer, ref PackUnpack.UnsafeGetAndAdvance(ref Buf, (int)length), length);
    }

    public void ReadBlock(in Span<byte> buffer)
    {
        ReadBlock(ref MemoryMarshal.GetReference(buffer), (uint)buffer.Length);
    }

    public void ReadBlock(byte[] data, int offset, int length)
    {
        ReadBlock(data.AsSpan(offset, length));
    }

    public void SkipBlock(ulong length)
    {
        if (length > (uint)Buf.Length)
        {
            if (Controller != null)
            {
                length -= (uint)Buf.Length;
                Buf = new();

                while (length > (uint)int.MaxValue + 1)
                {
                    if (Controller.SkipBlock(ref this, (uint)int.MaxValue + 1))
                        PackUnpack.ThrowEndOfStreamException();
                    length -= (uint)int.MaxValue + 1;
                }

                if (!Controller.SkipBlock(ref this, (uint)length))
                    return;
            }

            Buf = new();
            PackUnpack.ThrowEndOfStreamException();
        }

        PackUnpack.UnsafeAdvance(ref Buf, (int)length);
    }

    public void SkipBlock(uint length)
    {
        if (length > Buf.Length)
        {
            if (Controller != null)
            {
                length -= (uint)Buf.Length;
                Buf = new();
                if (!Controller.SkipBlock(ref this, length))
                    return;
            }

            Buf = new();
            PackUnpack.ThrowEndOfStreamException();
        }

        PackUnpack.UnsafeAdvance(ref Buf, (int)length);
    }

    public void SkipBlock(int length)
    {
        SkipBlock((uint)length);
    }

    public void ReadBlock(ByteBuffer buffer)
    {
        ReadBlock(buffer.AsSyncSpan());
    }

    [SkipLocalsInit]
    public Guid ReadGuid()
    {
        Span<byte> buffer = stackalloc byte[16];
        ReadBlock(ref MemoryMarshal.GetReference(buffer), 16);
        return new(buffer);
    }

    public void SkipGuid()
    {
        SkipBlock(16);
    }

    public float ReadSingle()
    {
        return BitConverter.Int32BitsToSingle(ReadInt32());
    }

    public void SkipSingle()
    {
        SkipInt32();
    }

    public double ReadDouble()
    {
        return BitConverter.Int64BitsToDouble(ReadInt64());
    }

    public float ReadFloat()
    {
        return BitConverter.Int32BitsToSingle(ReadInt32());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static unsafe Half Int16BitsToHalf(short value)
    {
        return *(Half*)&value;
    }

    public Half ReadHalf()
    {
        return Int16BitsToHalf(ReadInt16());
    }

    public void SkipDouble()
    {
        SkipInt64();
    }

    public void SkipFloat()
    {
        SkipInt32();
    }

    public void SkipHalf()
    {
        SkipInt16();
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
                first = (ulong)ReadInt64();
                break;
            case 3:
                second = (uint)ReadInt32();
                first = (ulong)ReadInt64();
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
                SkipInt64();
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
        var bytes = new byte[length - 1];
        ReadBlock(bytes);
        return bytes;
    }

    public void SkipByteArray()
    {
        var length = ReadVUInt32();
        if (length == 0) return;
        SkipBlock(length - 1);
    }

    public ReadOnlySpan<byte> ReadByteArrayAsSpan()
    {
        Debug.Assert(Controller == null);
        var length = ReadVUInt32();
        if (length-- <= 1) return new();
        if (length > Buf.Length) PackUnpack.ThrowEndOfStreamException();
        var res = Buf[..(int)length];
        PackUnpack.UnsafeAdvance(ref Buf, (int)length);
        return res;
    }

    public ReadOnlyMemory<byte> ReadByteArrayAsMemory()
    {
        var length = ReadVUInt32();
        if (length-- <= 1) return new();
        return ReadBlockAsMemory(length);
    }

    ReadOnlyMemory<byte> ReadBlockAsMemory(uint length)
    {
        if (OriginalMemory.Length != 0)
        {
            var pos = GetCurrentPositionWithoutController();
            PackUnpack.UnsafeAdvance(ref Buf, (int)length);
            return OriginalMemory.Slice((int)pos, (int)length);
        }

        if (Controller != null && Controller.TryReadBlockAsMemory(ref this, length, out var res))
        {
            return res;
        }

        var resBuffer = new byte[length];
        ReadBlock(resBuffer.AsSpan());
        return resBuffer;
    }

    public byte[] ReadByteArrayRawTillEof()
    {
        var res = Buf.ToArray();
        PackUnpack.UnsafeAdvance(ref Buf, Buf.Length);
        return res;
    }

    public ReadOnlyMemory<byte> ReadByteArrayRawTillEofAsMemory()
    {
        return ReadBlockAsMemory((uint)Buf.Length);
    }

    public byte[] ReadByteArrayRaw(int len)
    {
        var res = new byte[len];
        ReadBlock(res);
        return res;
    }

    [SkipLocalsInit]
    public bool CheckMagic(ReadOnlySpan<byte> magic)
    {
        if (Buf.Length >= magic.Length)
        {
            if (!Buf.Slice(0, magic.Length).SequenceEqual(magic)) return false;
            PackUnpack.UnsafeAdvance(ref Buf, magic.Length);
            return true;
        }

        if (Controller == null) return false;

        Span<byte> buf = stackalloc byte[magic.Length];
        try
        {
            ReadBlock(ref MemoryMarshal.GetReference(buf), (uint)buf.Length);
            return buf.SequenceEqual(magic);
        }
        catch (EndOfStreamException)
        {
            return false;
        }
    }

    public bool CheckMagic(byte[] magic)
    {
        return CheckMagic(magic.AsSpan());
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
                SkipInt32();
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
            a[i] = ReadString()!;
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

    public void CopyAbsoluteToWriter(uint start, uint len, ref SpanWriter writer)
    {
        Debug.Assert(Controller == null);
        writer.WriteBlock(Original.Slice((int)start, (int)len));
    }

    public void CopyFromPosToWriter(uint start, ref SpanWriter writer)
    {
        Debug.Assert(Controller == null);
        writer.WriteBlock(Original.Slice((int)start, (int)GetCurrentPositionWithoutController() - (int)start));
    }
}
