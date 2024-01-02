using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using System.Text;
using BTDB.Buffer;
using Microsoft.Extensions.Primitives;

namespace BTDB.StreamLayer;

public struct MemWriter
{
    public nint Current;
    public nint Start;

    public nint End;

    // Is IMemWriter or byte[] when allocated array buffer on heap
    public object? Controller;

    // Span must be to pinned memory or stack
    unsafe MemWriter(Span<byte> buf)
    {
        Current = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buf));
        Start = Current;
        End = Current + buf.Length;
    }

    public static MemWriter CreateFromStackAllocatedSpan(Span<byte> buf)
    {
        return new MemWriter(buf);
    }

    public static MemWriter CreateFromPinnedSpan(Span<byte> buf)
    {
        return new MemWriter(buf);
    }

    // buf is pointer to pinned memory or native memory
    public MemWriter(nint buf, nint length)
    {
        Debug.Assert(length >= 0);
        Current = buf;
        Start = buf;
        End = buf + length;
    }

    public unsafe MemWriter(void* buf, int length)
    {
        Debug.Assert(length >= 0);
        Current = (nint)buf;
        Start = (nint)buf;
        End = (nint)buf + length;
    }

    public MemWriter()
    {
        Current = 0;
        Start = 0;
        End = 0;
    }

    public MemWriter(IMemWriter? controller)
    {
        Controller = controller;
        if (controller == null)
        {
            Current = 0;
            Start = 0;
            End = 0;
        }
        else
        {
            controller.Init(ref this);
        }
    }

    public void Flush()
    {
        if (Controller is IMemWriter controller)
        {
            controller.Flush(ref this, 0);
            return;
        }

        ThrowCanBeUsedOnlyWithController();
    }

    public unsafe ReadOnlySpan<byte> GetSpan()
    {
#if DEBUG
        if (Controller is IMemWriter) ThrowCannotBeUsedWithController();
#endif
        return new((void*)Start, (int)(Current - Start));
    }

    public unsafe ReadOnlySpan<byte> GetScopedSpanAndReset()
    {
#if DEBUG
        if (Controller is IMemWriter) ThrowCannotBeUsedWithController();
#endif
        ReadOnlySpan<byte> res = new((void*)Start, (int)(Current - Start));
        Start = Current;
        return res;
    }

    public unsafe ReadOnlySpan<byte> GetPersistentSpanAndReset()
    {
        if (Controller is IMemWriter) ThrowCannotBeUsedWithController();
        var data = new Span<byte>((void*)Start, (int)(Current - Start));
        Current = Start;
        var res = new Span<byte>(GC.AllocateUninitializedArray<byte>(data.Length));
        data.CopyTo(res);
        return res;
    }

    public unsafe Memory<byte> GetPersistentMemoryAndReset()
    {
        if (Controller is IMemWriter) ThrowCannotBeUsedWithController();
        if (Controller is byte[] arr)
        {
            var res = MemoryMarshal.CreateFromPinnedArray(arr,
                (int)(Start - (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(arr))),
                (int)(Current - Start));
            Controller = null;
            Start = 0;
            Current = 0;
            End = 0;
            return res;
        }
        else
        {
            var data = new Span<byte>((void*)Start, (int)(Current - Start));
            Current = Start;
            var res = new Memory<byte>(GC.AllocateUninitializedArray<byte>(data.Length));
            data.CopyTo(res.Span);
            return res;
        }
    }

    static void ThrowCannotBeUsedWithController()
    {
        throw new InvalidOperationException("Cannot have controller");
    }

    public void Reset()
    {
#if DEBUG
        if (Controller is IMemWriter) ThrowCannotBeUsedWithController();
#endif
        Current = Start;
    }

    /// Returned span is valid only until next write to this MemWriter
    public unsafe ReadOnlySpan<byte> GetSpanAndReset()
    {
        var res = new Span<byte>((void*)Start, (int)(Current - Start));
        Reset();
        return res;
    }

    public ByteBuffer GetByteBufferAndReset()
    {
        return ByteBuffer.NewAsync(GetPersistentMemoryAndReset());
    }

    public readonly long GetCurrentPosition()
    {
        if (Controller is IMemWriter controller) return controller.GetCurrentPosition(this);
        return Current - Start;
    }

    public void SetCurrentPosition(long pos)
    {
        if (Controller is IMemWriter controller)
        {
            controller.SetCurrentPosition(ref this, pos);
            return;
        }

        Current = Start + (nint)pos;
    }

    static void ThrowCanBeUsedOnlyWithController()
    {
        throw new InvalidOperationException("Need controller");
    }

    public unsafe bool Resize(uint spaceNeeded)
    {
        if (End - Current > spaceNeeded) return false;
        if (Controller is IMemWriter controller)
        {
            controller.Flush(ref this, spaceNeeded);
            return End - Current < spaceNeeded;
        }

        var pos = Current - Start;
        var size = Math.Min(Math.Max(pos + (nint)spaceNeeded, Math.Max((End - Start) * 2, 128)), Array.MaxLength);
        if (pos + spaceNeeded > size) throw new OutOfMemoryException();
        var arr = GC.AllocateUninitializedArray<byte>((int)size, pinned: true);
        var newStart = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(arr));
        Unsafe.CopyBlockUnaligned((void*)newStart, (void*)Start, (uint)pos);
        Controller = arr;
        Start = newStart;
        Current = newStart + pos;
        End = newStart + size;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Resize1()
    {
        if (Current == End) Resize(1);
    }

    public unsafe void WriteByteZero()
    {
        Resize1();
        Unsafe.Write<byte>((void*)Current, 0);
        Current++;
    }

    public unsafe void WriteBool(bool value)
    {
        Resize1();
        Unsafe.Write((void*)Current, value ? (byte)1 : (byte)0);
        Current++;
    }

    public unsafe void WriteUInt8(byte value)
    {
        Resize1();
        Unsafe.Write((void*)Current, value);
        Current++;
    }

    public unsafe void WriteInt8(sbyte value)
    {
        Resize1();
        Unsafe.Write((void*)Current, (byte)value);
        Current++;
    }

    public unsafe void WriteInt8Ordered(sbyte value)
    {
        Resize1();
        Unsafe.Write((void*)Current, (byte)(value + 128));
        Current++;
    }

    public void WriteVInt16(short value)
    {
        WriteVInt64(value);
    }

    public void WriteVUInt16(ushort value)
    {
        WriteVUInt64(value);
    }

    public void WriteVInt32(int value)
    {
        WriteVInt64(value);
    }

    public void WriteVUInt32(uint value)
    {
        WriteVUInt64(value);
    }

    [SkipLocalsInit]
    public unsafe void WriteVInt64(long value)
    {
        var len = PackUnpack.LengthVInt(value);
        if (Resize(len))
        {
            Span<byte> buf = stackalloc byte[(int)len];
            PackUnpack.UnsafePackVInt(ref MemoryMarshal.GetReference(buf), value, len);
            WriteBlock(buf);
            return;
        }

        PackUnpack.UnsafePackVInt(ref Unsafe.AsRef<byte>((void*)Current), value, len);
        Current += (nint)len;
    }

    [SkipLocalsInit]
    public unsafe void WriteVUInt64(ulong value)
    {
        var len = PackUnpack.LengthVUInt(value);
        if (Resize(len))
        {
            Span<byte> buf = stackalloc byte[(int)len];
            PackUnpack.UnsafePackVUInt(ref MemoryMarshal.GetReference(buf), value, len);
            WriteBlock(buf);
            return;
        }

        PackUnpack.UnsafePackVUInt(ref Unsafe.AsRef<byte>((void*)Current), value, len);
        Current += (nint)len;
    }

    [SkipLocalsInit]
    public unsafe void WriteInt64BE(long value)
    {
        if (Resize(8))
        {
            var buf = stackalloc byte[8];
            Unsafe.WriteUnaligned(buf, PackUnpack.AsBigEndian((ulong)value));
            WriteBlock(buf, 8);
            return;
        }

        Unsafe.WriteUnaligned((void*)Current, PackUnpack.AsBigEndian((ulong)value));
        Current += 8;
    }

    [SkipLocalsInit]
    public unsafe void WriteInt64LE(long value)
    {
        if (Resize(8))
        {
            var buf = stackalloc byte[8];
            Unsafe.WriteUnaligned(buf, PackUnpack.AsLittleEndian((ulong)value));
            WriteBlock(buf, 8);
            return;
        }

        Unsafe.WriteUnaligned((void*)Current, PackUnpack.AsLittleEndian((ulong)value));
        Current += 8;
    }

    [SkipLocalsInit]
    public unsafe void WriteUInt16LE(ushort value)
    {
        if (Resize(2))
        {
            var buf = stackalloc byte[2];
            Unsafe.WriteUnaligned(buf, PackUnpack.AsLittleEndian(value));
            WriteBlock(buf, 2);
            return;
        }

        Unsafe.WriteUnaligned((void*)Current, PackUnpack.AsLittleEndian(value));
        Current += 2;
    }

    [SkipLocalsInit]
    public unsafe void WriteInt32BE(int value)
    {
        if (Resize(4))
        {
            var buf = stackalloc byte[4];
            Unsafe.WriteUnaligned(buf, PackUnpack.AsBigEndian((uint)value));
            WriteBlock(buf, 4);
            return;
        }

        Unsafe.WriteUnaligned((void*)Current, PackUnpack.AsBigEndian((uint)value));
        Current += 4;
    }

    [SkipLocalsInit]
    public unsafe void WriteInt16BE(short value)
    {
        if (Resize(2))
        {
            var buf = stackalloc byte[2];
            Unsafe.WriteUnaligned(buf, PackUnpack.AsBigEndian((ushort)value));
            WriteBlock(buf, 2);
            return;
        }

        Unsafe.WriteUnaligned((void*)Current, PackUnpack.AsBigEndian((ushort)value));
        Current += 2;
    }

    [SkipLocalsInit]
    public unsafe void WriteInt32LE(int value)
    {
        if (Resize(4))
        {
            var buf = stackalloc byte[4];
            Unsafe.WriteUnaligned(buf, PackUnpack.AsLittleEndian((uint)value));
            WriteBlock(buf, 4);
            return;
        }

        Unsafe.WriteUnaligned((void*)Current, PackUnpack.AsLittleEndian((uint)value));
        Current += 4;
    }

    [SkipLocalsInit]
    public unsafe void WriteUInt32LE(uint value)
    {
        if (Resize(4))
        {
            var buf = stackalloc byte[4];
            Unsafe.WriteUnaligned(buf, PackUnpack.AsLittleEndian(value));
            WriteBlock(buf, 4);
            return;
        }

        Unsafe.WriteUnaligned((void*)Current, PackUnpack.AsLittleEndian(value));
        Current += 4;
    }

    [SkipLocalsInit]
    public unsafe void WriteUInt64LE(ulong value)
    {
        if (Resize(8))
        {
            var buf = stackalloc byte[8];
            Unsafe.WriteUnaligned(buf, PackUnpack.AsLittleEndian(value));
            WriteBlock(buf, 8);
            return;
        }

        Unsafe.WriteUnaligned((void*)Current, PackUnpack.AsLittleEndian(value));
        Current += 8;
    }

    [SkipLocalsInit]
    public unsafe void WriteUInt64BE(ulong value)
    {
        if (Resize(8))
        {
            var buf = stackalloc byte[8];
            Unsafe.WriteUnaligned(buf, PackUnpack.AsBigEndian(value));
            WriteBlock(buf, 8);
            return;
        }

        Unsafe.WriteUnaligned((void*)Current, PackUnpack.AsBigEndian(value));
        Current += 8;
    }

    public void WriteDateTime(DateTime value)
    {
        WriteInt64BE(value.ToBinary());
    }

    public void WriteDateTimeForbidUnspecifiedKind(DateTime value)
    {
        if (value.Kind == DateTimeKind.Unspecified)
        {
            if (value == DateTime.MinValue)
                value = DateTime.MinValue.ToUniversalTime();
            else if (value == DateTime.MaxValue)
                value = DateTime.MaxValue.ToUniversalTime();
            else
                throw new ArgumentOutOfRangeException(nameof(value),
                    "DateTime.Kind cannot be stored as Unspecified");
        }

        WriteDateTime(value);
    }

    public void WriteDateTimeOffset(DateTimeOffset value)
    {
        WriteVInt64(value.Ticks);
        WriteTimeSpan(value.Offset);
    }

    public void WriteTimeSpan(TimeSpan value)
    {
        WriteVInt64(value.Ticks);
    }

    public unsafe void WriteString(string? value)
    {
        if (value == null)
        {
            WriteByteZero();
            return;
        }

        var l = value.Length;
        if (l == 0)
        {
            WriteUInt8(1);
            return;
        }

        Resize((uint)l + 4);

        WriteVUInt32((uint)(l + 1));

        fixed (char* strPtrStart = value)
        {
            var strPtr = strPtrStart;
            var strPtrEnd = strPtrStart + l;
            goFast:
            while (BitConverter.IsLittleEndian && strPtr + 4 <= strPtrEnd && Current + 4 <= End)
            {
                var c4Data = Unsafe.Read<ulong>(strPtr);
                if (!PackUnpack.AllCharsInUInt64AreAscii(c4Data))
                {
                    if (PackUnpack.AllCharsInUInt32AreAscii((uint)c4Data))
                    {
                        Unsafe.WriteUnaligned((void*)Current, (ushort)(((uint)c4Data >> 8) | c4Data));
                        Current += 2;
                        strPtr += 2;
                        c4Data >>= 32;
                    }

                    if ((c4Data & 0xff80) == 0)
                    {
                        Unsafe.Write((void*)Current, (byte)c4Data);
                        Current++;
                        strPtr++;
                    }

                    break;
                }

                Unsafe.WriteUnaligned((void*)Current, PackUnpack.NarrowFourUtf16CharsToAscii(c4Data));
                Current += 4;
                strPtr += 4;

                if (Vector128.IsHardwareAccelerated && strPtr + 8 <= strPtrEnd && Current + 8 <= End)
                {
                    var v = Unsafe.ReadUnaligned<Vector128<ushort>>(strPtr);
                    if (!PackUnpack.AllCharsInVectorAreAscii(v)) continue;
                    Unsafe.WriteUnaligned((void*)Current, PackUnpack.NarrowEightUtf16CharsToAscii(v));
                    Current += 8;
                    strPtr += 8;
                    while (strPtr + 16 <= strPtrEnd && Current + 16 <= End)
                    {
                        v = Unsafe.ReadUnaligned<Vector128<ushort>>(strPtr);
                        var v2 = Unsafe.ReadUnaligned<Vector128<ushort>>(strPtr + 8);
                        if (!PackUnpack.AllCharsInVectorAreAscii(v | v2))
                        {
                            if (!PackUnpack.AllCharsInVectorAreAscii(v)) break;
                            Unsafe.WriteUnaligned((void*)Current, PackUnpack.NarrowEightUtf16CharsToAscii(v));
                            Current += 8;
                            strPtr += 8;
                            break;
                        }

                        Unsafe.WriteUnaligned((void*)Current,
                            PackUnpack.NarrowSixteenUtf16CharsToAscii(v, v2));
                        Current += 16;
                        strPtr += 16;
                    }
                }
            }

            while (strPtr != strPtrEnd)
            {
                var c = *strPtr++;
                if (c < 0x80)
                {
                    Resize1();
                    Unsafe.AsRef<byte>((void*)Current) = (byte)c;
                    Current++;
                    goto goFast;
                }

                if (char.IsHighSurrogate(c) && strPtr != strPtrEnd)
                {
                    var c2 = *strPtr;
                    if (char.IsLowSurrogate(c2))
                    {
                        WriteVUInt32((uint)((c - 0xD800) * 0x400 + (c2 - 0xDC00) + 0x10000));
                        strPtr++;
                        continue;
                    }
                }

                WriteVUInt32(c);
            }
        }
    }

    public void WriteStringOrdered(string? value)
    {
        if (value == null)
        {
            WriteVUInt32(0x110001);
            return;
        }

        WriteStringOrderedPrefix(value);
        WriteByteZero();
    }

    public unsafe void WriteStringOrderedPrefix(string value)
    {
        var l = value.Length;
        Resize((uint)l + 1);

        fixed (char* strPtrStart = value)
        {
            var strPtr = strPtrStart;
            var strPtrEnd = strPtrStart + l;
            goFast:
            while (BitConverter.IsLittleEndian && strPtr + 4 <= strPtrEnd && Current + 4 <= End)
            {
                var c4Data = Unsafe.Read<ulong>(strPtr);
                if (!PackUnpack.AllCharsInUInt64AreAsciiM1(ref c4Data))
                {
                    var c2Data = (uint)c4Data;
                    if (PackUnpack.AllCharsInUInt32AreAsciiM1(ref c2Data))
                    {
                        Unsafe.WriteUnaligned((void*)Current, (ushort)((c2Data >> 8) | c2Data));
                        Current += 2;
                        strPtr += 2;
                        c4Data >>= 32;
                    }

                    if ((c4Data & 0xffff) < 0x7f)
                    {
                        Unsafe.Write((void*)Current, (byte)(c4Data + 1));
                        Current++;
                        strPtr++;
                    }

                    break;
                }

                Unsafe.WriteUnaligned((void*)Current, PackUnpack.NarrowFourUtf16CharsToAscii(c4Data));
                Current += 4;
                strPtr += 4;

                if ((Sse2.IsSupported || AdvSimd.IsSupported) && strPtr + 8 <= strPtrEnd && Current + 8 <= End)
                {
                    var v = PackUnpack.Add1Saturate(Unsafe.ReadUnaligned<Vector128<ushort>>(strPtr));
                    if (!PackUnpack.AllCharsInVectorAreAscii(v)) continue;
                    Unsafe.WriteUnaligned((void*)Current, PackUnpack.NarrowEightUtf16CharsToAscii(v));
                    Current += 8;
                    strPtr += 8;
                    while (strPtr + 16 <= strPtrEnd && Current + 16 <= End)
                    {
                        v = PackUnpack.Add1Saturate(Unsafe.ReadUnaligned<Vector128<ushort>>(strPtr));
                        var v2 = PackUnpack.Add1Saturate(Unsafe.ReadUnaligned<Vector128<ushort>>(strPtr + 8));
                        if (!PackUnpack.AllCharsInVectorAreAscii(v | v2))
                        {
                            if (!PackUnpack.AllCharsInVectorAreAscii(v)) break;
                            Unsafe.WriteUnaligned((void*)Current, PackUnpack.NarrowEightUtf16CharsToAscii(v));
                            Current += 8;
                            strPtr += 8;
                            break;
                        }

                        Unsafe.WriteUnaligned((void*)Current,
                            PackUnpack.NarrowSixteenUtf16CharsToAscii(v, v2));
                        Current += 16;
                        strPtr += 16;
                    }
                }
            }

            while (strPtr != strPtrEnd)
            {
                var c = *strPtr++;
                if (c < 0x7f)
                {
                    Resize1();
                    Unsafe.Write((void*)Current, (byte)(c + 1));
                    Current++;
                    goto goFast;
                }

                if (char.IsHighSurrogate(c) && strPtr != strPtrEnd)
                {
                    var c2 = *strPtr;
                    if (char.IsLowSurrogate(c2))
                    {
                        WriteVUInt32((uint)((c - 0xD800) * 0x400 + (c2 - 0xDC00) + 0x10001));
                        strPtr++;
                        continue;
                    }
                }

                WriteVUInt32((uint)c + 1);
            }
        }
    }

    public unsafe void WriteStringInUtf8(string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        var len = PackUnpack.LengthVUInt((uint)byteCount);
        Resize(len + (uint)byteCount);
        PackUnpack.UnsafePackVUInt(ref Unsafe.AsRef<byte>((void*)Current), (uint)byteCount, len);
        Current += (nint)len;
        if (Current + byteCount <= End)
        {
            fixed (char* valueChars = value)
            {
                Encoding.UTF8.GetBytes(valueChars, value.Length, (byte*)Current, byteCount);
            }

            Current += byteCount;
            return;
        }

        var encoder = Encoding.UTF8.GetEncoder();
        var charLen = value.Length;
        fixed (char* valueChars = value)
        {
            var charsPtr = valueChars;
            do
            {
                encoder.Convert(charsPtr, charLen, (byte*)Current, (int)(End - Current), true, out var charsUsed,
                    out var bytesUsed, out var completed);
                Current += bytesUsed;
                if (completed) break;
                charsPtr += charsUsed;
                charLen -= charsUsed;
                Resize1();
            } while (true);
        }
    }

    public void WriteBlock(scoped ReadOnlySpan<byte> data)
    {
        WriteBlock(ref MemoryMarshal.GetReference(data), (uint)data.Length);
    }

    public unsafe void WriteBlock(ref byte buffer, uint length)
    {
        if (Current + length > End)
        {
            if (Controller is IMemWriter controller)
            {
                var bufLength = End - Current;
                if (bufLength > 0)
                {
                    fixed (byte* bufferPtr = &buffer)
                    {
                        Unsafe.CopyBlockUnaligned((void*)Current, bufferPtr, (uint)bufLength);
                    }

                    Current = End;
                    buffer = ref Unsafe.AddByteOffset(ref buffer, bufLength);
                    length -= (uint)bufLength;
                }

                controller.WriteBlock(ref this, ref buffer, length);
                return;
            }

            Resize(length); // returns always success because it is without controller
        }

        fixed (byte* bufferPtr = &buffer)
        {
            Unsafe.CopyBlockUnaligned((void*)Current, bufferPtr, length);
        }

        Current += (nint)length;
    }

    public void WriteBlock(byte[] buffer, int offset, int length)
    {
        WriteBlock(buffer.AsSpan(offset, length));
    }

    public unsafe void WriteBlock(nint data, int length)
    {
        WriteBlock(ref Unsafe.AsRef<byte>((void*)data), (uint)length);
    }

    public unsafe void WriteBlock(byte* data, int length)
    {
        if (Current + length > End)
        {
            if (Controller is IMemWriter controller)
            {
                var bufLength = End - Current;
                if (bufLength > 0)
                {
                    Unsafe.CopyBlockUnaligned((void*)Current, data, (uint)bufLength);
                    Current = End;
                    data += bufLength;
                    length -= (int)bufLength;
                }

                controller.WriteBlock(ref this, ref Unsafe.AsRef<byte>(data), (nuint)length);
                return;
            }

            Resize((uint)length); // returns always success because it is without controller
        }

        Unsafe.CopyBlockUnaligned((void*)Current, data, (uint)length);
        Current += length;
    }

    public void WriteBlock(byte[] data)
    {
        WriteBlock(data.AsSpan());
    }

    public unsafe void WriteGuid(Guid value)
    {
        WriteBlock((byte*)&value, 16);
    }

    public void WriteSingle(float value)
    {
        WriteInt32BE(BitConverter.SingleToInt32Bits(value));
    }

    public void WriteDouble(double value)
    {
        WriteInt64BE(BitConverter.DoubleToInt64Bits(value));
    }

    public void WriteDoubleOrdered(double value)
    {
        var i = BitConverter.DoubleToUInt64Bits(value);
        if (i < 0x8000_0000_0000_0000UL) i += 0x8000_0000_0000_0000UL;
        else i ^= ulong.MaxValue;
        WriteInt64BE((long)i);
    }

    public void WriteHalf(Half value)
    {
        WriteInt16BE(Unsafe.BitCast<Half, short>(value));
    }

    [SkipLocalsInit]
    public void WriteDecimal(decimal value)
    {
        Span<int> ints = stackalloc int[4];
        decimal.GetBits(value, ints);
        var header = (byte)((ints[3] >> 16) & 31);
        if (ints[3] < 0) header |= 128;
        var first = (uint)ints[0] + ((ulong)ints[1] << 32);
        if (ints[2] == 0)
        {
            if (first == 0)
            {
                WriteUInt8(header);
            }
            else
            {
                header |= 32;
                WriteUInt8(header);
                WriteVUInt64(first);
            }
        }
        else
        {
            if ((uint)ints[2] < 0x10000000)
            {
                header |= 64;
                WriteUInt8(header);
                WriteVUInt32((uint)ints[2]);
                WriteInt64BE((long)first);
            }
            else
            {
                header |= 64 | 32;
                WriteUInt8(header);
                WriteInt32BE(ints[2]);
                WriteInt64BE((long)first);
            }
        }
    }

    public void WriteByteArray(byte[]? value)
    {
        if (value == null)
        {
            WriteByteZero();
            return;
        }

        WriteVUInt32((uint)(value.Length + 1));
        WriteBlock(value);
    }

    public void WriteByteArray(ByteBuffer value)
    {
        WriteVUInt32((uint)(value.Length + 1));
        WriteBlock(value);
    }

    public void WriteByteArray(ReadOnlySpan<byte> value)
    {
        WriteVUInt32((uint)(value.Length + 1));
        WriteBlock(value);
    }

    public void WriteByteArray(ReadOnlyMemory<byte> value)
    {
        WriteByteArray(value.Span);
    }

    public void WriteByteArrayLength(ReadOnlyMemory<byte> value)
    {
        WriteVUInt32(1 + (uint)value.Length);
    }

    public void WriteByteArrayRaw(byte[]? value)
    {
        if (value == null) return;
        WriteBlock(value);
    }

    public void WriteBlock(ByteBuffer data)
    {
        WriteBlock(data.AsSyncReadOnlySpan());
    }

    public void WriteBlock(ReadOnlyMemory<byte> data)
    {
        WriteBlock(data.Span);
    }

    [SkipLocalsInit]
    public void WriteIPAddress(IPAddress? value)
    {
        if (value == null)
        {
            WriteUInt8(3);
            return;
        }

        if (value.AddressFamily == AddressFamily.InterNetworkV6)
        {
            if (value.ScopeId != 0)
            {
                Span<byte> buf = stackalloc byte[16];
                ref var bufRef = ref MemoryMarshal.GetReference(buf);
                value.TryWriteBytes(buf, out _);
                WriteUInt8(2);
                WriteBlock(ref bufRef, 16);
                WriteVUInt64((ulong)value.ScopeId);
            }
            else
            {
                Span<byte> buf = stackalloc byte[16];
                ref var bufRef = ref MemoryMarshal.GetReference(buf);
                value.TryWriteBytes(buf, out _);
                WriteUInt8(1);
                WriteBlock(ref bufRef, 16);
            }
        }
        else
        {
            WriteUInt8(0);
#pragma warning disable 612, 618
            WriteInt32LE((int)value.Address);
#pragma warning restore 612, 618
        }
    }

    public void WriteVersion(Version? value)
    {
        if (value == null)
        {
            WriteUInt8(0);
            return;
        }

        WriteVUInt32((uint)value.Major + 1);
        WriteVUInt32((uint)value.Minor + 1);
        if (value.Minor == -1) return;
        WriteVUInt32((uint)value.Build + 1);
        if (value.Build == -1) return;
        WriteVUInt32((uint)value.Revision + 1);
    }

    public void WriteStringValues(StringValues value)
    {
        WriteVUInt32((uint)value.Count);
        foreach (var s in value)
        {
            WriteString(s);
        }
    }

    public uint NoControllerGetCurrentPosition()
    {
        Debug.Assert(Controller is not IMemWriter);
        return (uint)(Current - Start);
    }

    public void NoControllerSetCurrentPosition(uint pos)
    {
        Debug.Assert(Controller is not IMemWriter);
        Current = Start + (nint)pos;
    }

    public uint StartWriteByteArray()
    {
        if (Controller is IMemWriter) ThrowCannotBeUsedWithController();
        WriteByteZero();
        return NoControllerGetCurrentPosition();
    }

    public unsafe void FinishWriteByteArray(uint start)
    {
        var end = NoControllerGetCurrentPosition();
        var len = end - start + 1;
        var lenOfLen = PackUnpack.LengthVUInt(len);
        if (lenOfLen == 1)
        {
            Unsafe.Write((byte*)Start + (start - 1), (byte)len);
            return;
        }

        // Reserve space at end
        Resize(lenOfLen - 1);
        Current += (int)lenOfLen - 1;
        // Make Space By Moving Memory
        System.Buffer.MemoryCopy((byte*)Start + start, (byte*)Start + start + lenOfLen - 1, len - 1, len - 1);
        // Update Length at start
        PackUnpack.UnsafePackVUInt(ref Unsafe.AsRef<byte>((byte*)Start + start - 1), len, lenOfLen);
    }

    unsafe Span<byte> InternalGetSpan(uint start, uint len)
    {
        return MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>((byte*)Start + start), (int)len);
    }

    public uint StartXor()
    {
        if (Controller is IMemWriter) ThrowCannotBeUsedWithController();
        return NoControllerGetCurrentPosition();
    }

    public void FinishXor(uint start, byte value = 0xFF)
    {
        var data = InternalGetSpan(start, NoControllerGetCurrentPosition() - start);
        if (Vector.IsHardwareAccelerated && data.Length >= Vector<byte>.Count)
        {
            var dataVector = MemoryMarshal.Cast<byte, Vector<byte>>(data);
            var vectorFFs = new Vector<byte>(value);
            for (var i = 0; i < dataVector.Length; i++)
            {
                dataVector[i] ^= vectorFFs;
            }

            data = data[(Vector<byte>.Count * dataVector.Length)..];
        }

        for (var i = 0; i < data.Length; i++)
        {
            data[i] ^= value;
        }
    }

    public void UpdateBuffer(ReadOnlySpan<byte> writtenBuffer)
    {
        Reset();
        WriteBlock(writtenBuffer);
    }

    public unsafe Span<byte> BlockWriteToSpan(int length, out bool needToBeWritten)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (Resize((uint)length))
        {
            needToBeWritten = true;
            return GC.AllocateUninitializedArray<byte>(length);
        }

        var res = new Span<byte>((void*)Current, length);
        Current += length;
        needToBeWritten = false;
        return res;
    }

    public readonly unsafe ReadOnlySpan<byte> AsReadOnlySpan(int ofs, int len)
    {
        return new((void*)(Start + ofs), len);
    }
}
