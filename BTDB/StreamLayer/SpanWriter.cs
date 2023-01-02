using System;
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

public ref struct SpanWriter
{
    public SpanWriter(Span<byte> initialBuffer)
    {
        Buf = initialBuffer;
        InitialBuffer = initialBuffer;
        HeapBuffer = null;
        Controller = null;
    }

    public unsafe SpanWriter(void* buf, int size)
    {
        Buf = MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>(buf), size);
        InitialBuffer = Buf;
        HeapBuffer = null;
        Controller = null;
    }

    public SpanWriter(ISpanWriter controller)
    {
        Controller = controller;
        Buf = new Span<byte>();
        InitialBuffer = new Span<byte>();
        HeapBuffer = null;
        Controller.Init(ref this);
    }

    public Span<byte> Buf;
    public Span<byte> InitialBuffer;

    // When this is not null than Buf must be slice of HeapBuffer.AsSpan(), otherwise Buf must be slice of InitialBuffer
    public byte[]? HeapBuffer;
    public ISpanWriter? Controller;

    public ReadOnlySpan<byte> GetSpan()
    {
#if DEBUG
        if (Controller != null) ThrowCannotBeUsedWithController();
#endif
        if (HeapBuffer != null)
        {
            return HeapBuffer.AsSpan(0, HeapBuffer.Length - Buf.Length);
        }

        return InitialBuffer[..^Buf.Length];
    }

    public ReadOnlySpan<byte> GetPersistentSpanAndReset()
    {
        if (Controller != null) ThrowCannotBeUsedWithController();
        if (HeapBuffer != null)
        {
            var res = HeapBuffer.AsSpan(0, HeapBuffer.Length - Buf.Length);
            Buf = InitialBuffer;
            HeapBuffer = null;
            return res;
        }
        else
        {
            var res = InitialBuffer[..^Buf.Length];
            InitialBuffer = Buf;
            return res;
        }
    }

    public Memory<byte> GetPersistentMemoryAndReset()
    {
        if (Controller != null) ThrowCannotBeUsedWithController();
        if (HeapBuffer != null)
        {
            var res = HeapBuffer.AsMemory(0, HeapBuffer.Length - Buf.Length);
            Buf = InitialBuffer;
            HeapBuffer = null;
            return res;
        }
        else
        {
            var res = InitialBuffer[..^Buf.Length].ToArray();
            Buf = InitialBuffer;
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
        if (Controller != null) ThrowCannotBeUsedWithController();
#endif
        Buf = HeapBuffer ?? InitialBuffer;
    }

    public ReadOnlySpan<byte> GetSpanAndReset()
    {
#if DEBUG
        if (Controller != null) ThrowCannotBeUsedWithController();
#endif
        if (HeapBuffer != null)
        {
            var buf = HeapBuffer;
            var res = buf.AsSpan(0, buf.Length - Buf.Length);
            HeapBuffer = null;
            Buf = InitialBuffer;
            return res;
        }
        else
        {
            var res = InitialBuffer[..^Buf.Length];
            Buf = InitialBuffer;
            return res;
        }
    }

    public ByteBuffer GetByteBufferAndReset()
    {
        if (Controller != null) ThrowCannotBeUsedWithController();
        if (HeapBuffer != null)
        {
            var buf = HeapBuffer;
            var res = ByteBuffer.NewAsync(buf, 0, buf.Length - Buf.Length);
            HeapBuffer = null;
            Buf = InitialBuffer;
            return res;
        }
        else
        {
            var res = ByteBuffer.NewAsync(InitialBuffer[..^Buf.Length]);
            Buf = InitialBuffer;
            return res;
        }
    }

    public long GetCurrentPosition()
    {
        if (Controller != null) return Controller.GetCurrentPosition(this);

        if (HeapBuffer != null)
        {
            return HeapBuffer.Length - Buf.Length;
        }

        return InitialBuffer.Length - Buf.Length;
    }

    public void SetCurrentPosition(long pos)
    {
        if (Controller != null)
        {
            Controller.SetCurrentPosition(ref this, pos);
            return;
        }

        if (HeapBuffer != null)
        {
            Buf = HeapBuffer.AsSpan((int)pos);
            return;
        }

        Buf = InitialBuffer[(int)pos..];
    }

    /// <summary>
    /// DANGER: This can be called only as last method on this `SpanWriter`!
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

    bool Resize(uint spaceNeeded)
    {
        if (Controller != null)
        {
            return Controller.Flush(ref this);
        }

        var pos = (uint)GetCurrentPosition();
        if (HeapBuffer == null)
        {
            var newSize = Math.Max((uint)InitialBuffer.Length * 2, 32);
            newSize = Math.Max(newSize, pos + spaceNeeded);
            newSize = Math.Min(newSize, (uint)Array.MaxLength);
            HeapBuffer = new byte[newSize];
            InitialBuffer[..(int)pos].CopyTo(HeapBuffer);
            Buf = HeapBuffer.AsSpan((int)pos, (int)(newSize - pos));
        }
        else
        {
            var newSize = Math.Max((uint)HeapBuffer.Length * 2, pos + spaceNeeded);
            newSize = Math.Min(newSize, (uint)Array.MaxLength);
            Array.Resize(ref HeapBuffer, (int)newSize);
            Buf = HeapBuffer.AsSpan((int)pos, (int)(newSize - pos));
        }

        return true;
    }

    void TryReserve(uint spaceNeeded)
    {
        if (Buf.Length < spaceNeeded) Resize(spaceNeeded);
    }

    public void WriteByteZero()
    {
        if (Buf.IsEmpty)
        {
            Resize(1);
        }

        PackUnpack.UnsafeGetAndAdvance(ref Buf, 1) = 0;
    }

    public unsafe void WriteBool(bool value)
    {
        if (Buf.IsEmpty)
        {
            Resize(1);
        }

        PackUnpack.UnsafeGetAndAdvance(ref Buf, 1) = *(byte*)&value;
    }

    public void WriteUInt8(byte value)
    {
        if (Buf.IsEmpty)
        {
            Resize(1);
        }

        PackUnpack.UnsafeGetAndAdvance(ref Buf, 1) = value;
    }

    public void WriteInt8(sbyte value)
    {
        if (Buf.IsEmpty)
        {
            Resize(1);
        }

        PackUnpack.UnsafeGetAndAdvance(ref Buf, 1) = (byte)value;
    }

    public void WriteInt8Ordered(sbyte value)
    {
        if (Buf.IsEmpty)
        {
            Resize(1);
        }

        PackUnpack.UnsafeGetAndAdvance(ref Buf, 1) = (byte)(value + 128);
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
    public void WriteVInt64(long value)
    {
        var len = PackUnpack.LengthVInt(value);
        if ((uint)Buf.Length < len)
        {
            if (!Resize(len))
            {
                Span<byte> buf = stackalloc byte[(int)len];
                ref var bufRef = ref MemoryMarshal.GetReference(buf);
                PackUnpack.UnsafePackVInt(ref bufRef, value, len);
                WriteBlock(ref bufRef, len);
                return;
            }
        }

        PackUnpack.UnsafePackVInt(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, (int)len), value, len);
    }

    [SkipLocalsInit]
    public void WriteVUInt64(ulong value)
    {
        var len = PackUnpack.LengthVUInt(value);
        if ((uint)Buf.Length < len)
        {
            if (!Resize(len))
            {
                Span<byte> buf = stackalloc byte[(int)len];
                ref var bufRef = ref MemoryMarshal.GetReference(buf);
                PackUnpack.UnsafePackVUInt(ref bufRef, value, len);
                WriteBlock(ref bufRef, len);
                return;
            }
        }

        PackUnpack.UnsafePackVUInt(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, (int)len), value, len);
    }

    [SkipLocalsInit]
    public void WriteInt64(long value)
    {
        if ((uint)Buf.Length < 8u)
        {
            if (!Resize(8))
            {
                Span<byte> buf = stackalloc byte[8];
                ref var bufRef = ref MemoryMarshal.GetReference(buf);
                Unsafe.As<byte, ulong>(ref bufRef) = PackUnpack.AsBigEndian((ulong)value);
                WriteBlock(ref bufRef, 8);
                return;
            }
        }

        Unsafe.As<byte, ulong>(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, 8)) =
            PackUnpack.AsBigEndian((ulong)value);
    }

    [SkipLocalsInit]
    public void WriteUInt16LE(ushort value)
    {
        if ((uint)Buf.Length < 2u)
        {
            if (!Resize(2))
            {
                Span<byte> buf = stackalloc byte[2];
                ref var bufRef = ref MemoryMarshal.GetReference(buf);
                Unsafe.As<byte, ushort>(ref bufRef) = PackUnpack.AsLittleEndian(value);
                WriteBlock(ref bufRef, 2);
                return;
            }
        }

        Unsafe.As<byte, ushort>(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, 2)) =
            PackUnpack.AsLittleEndian(value);
    }

    [SkipLocalsInit]
    public void WriteInt32(int value)
    {
        if ((uint)Buf.Length < 4u)
        {
            if (!Resize(4))
            {
                Span<byte> buf = stackalloc byte[4];
                ref var bufRef = ref MemoryMarshal.GetReference(buf);
                Unsafe.As<byte, uint>(ref bufRef) = PackUnpack.AsBigEndian((uint)value);
                WriteBlock(ref bufRef, 4);
                return;
            }
        }

        Unsafe.As<byte, uint>(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, 4)) =
            PackUnpack.AsBigEndian((uint)value);
    }

    [SkipLocalsInit]
    public void WriteInt16(int value)
    {
        if ((uint)Buf.Length < 2u)
        {
            if (!Resize(2))
            {
                Span<byte> buf = stackalloc byte[2];
                ref var bufRef = ref MemoryMarshal.GetReference(buf);
                Unsafe.As<byte, ushort>(ref bufRef) = PackUnpack.AsBigEndian((ushort)value);
                WriteBlock(ref bufRef, 2);
                return;
            }
        }

        Unsafe.As<byte, ushort>(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, 2)) =
            PackUnpack.AsBigEndian((ushort)value);
    }

    [SkipLocalsInit]
    public void WriteInt32LE(int value)
    {
        if ((uint)Buf.Length < 4u)
        {
            if (!Resize(4))
            {
                Span<byte> buf = stackalloc byte[4];
                ref var bufRef = ref MemoryMarshal.GetReference(buf);
                Unsafe.As<byte, uint>(ref bufRef) = PackUnpack.AsLittleEndian((uint)value);
                WriteBlock(ref bufRef, 4);
                return;
            }
        }

        Unsafe.As<byte, uint>(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, 4)) =
            PackUnpack.AsLittleEndian((uint)value);
    }

    [SkipLocalsInit]
    public void WriteUInt32LE(uint value)
    {
        if ((uint)Buf.Length < 4u)
        {
            if (!Resize(4))
            {
                Span<byte> buf = stackalloc byte[4];
                ref var bufRef = ref MemoryMarshal.GetReference(buf);
                Unsafe.As<byte, uint>(ref bufRef) = PackUnpack.AsLittleEndian(value);
                WriteBlock(ref bufRef, 4);
                return;
            }
        }

        Unsafe.As<byte, uint>(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, 4)) =
            PackUnpack.AsLittleEndian(value);
    }

    public void WriteDateTime(DateTime value)
    {
        WriteInt64(value.ToBinary());
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

        TryReserve((uint)l + 4);

        WriteVUInt32((uint)(l + 1));

        fixed (char* strPtrStart = value)
        {
            var strPtr = strPtrStart;
            var strPtrEnd = strPtrStart + l;
            goFast:
            while (BitConverter.IsLittleEndian && strPtr + 4 <= strPtrEnd && Buf.Length >= 4)
            {
                var c4Data = Unsafe.Read<ulong>(strPtr);
                if (!PackUnpack.AllCharsInUInt64AreAscii(c4Data))
                {
                    if (PackUnpack.AllCharsInUInt32AreAscii((uint)c4Data))
                    {
                        PackUnpack.UnsafeGetAndAdvance<ushort>(ref Buf) =
                            (ushort)(((uint)c4Data >> 8) | c4Data);
                        strPtr += 2;
                        c4Data >>= 32;
                    }

                    if ((c4Data & 0xff80) == 0)
                    {
                        PackUnpack.UnsafeGetAndAdvance<byte>(ref Buf) = (byte)c4Data;
                        strPtr++;
                    }

                    break;
                }

                PackUnpack.UnsafeGetAndAdvance<uint>(ref Buf) = PackUnpack.NarrowFourUtf16CharsToAscii(c4Data);
                strPtr += 4;

                if (Vector128.IsHardwareAccelerated && strPtr + 8 <= strPtrEnd && Buf.Length >= 8)
                {
                    var v = Unsafe.ReadUnaligned<Vector128<ushort>>(strPtr);
                    if (!PackUnpack.AllCharsInVectorAreAscii(v)) continue;
                    PackUnpack.UnsafeGetAndAdvance<ulong>(ref Buf) = PackUnpack.NarrowEightUtf16CharsToAscii(v);
                    strPtr += 8;
                    while (strPtr + 16 <= strPtrEnd && Buf.Length >= 16)
                    {
                        v = Unsafe.ReadUnaligned<Vector128<ushort>>(strPtr);
                        var v2 = Unsafe.ReadUnaligned<Vector128<ushort>>(strPtr + 8);
                        if (!PackUnpack.AllCharsInVectorAreAscii(v | v2))
                        {
                            if (!PackUnpack.AllCharsInVectorAreAscii(v)) break;
                            PackUnpack.UnsafeGetAndAdvance<ulong>(ref Buf) = PackUnpack.NarrowEightUtf16CharsToAscii(v);
                            strPtr += 8;
                            break;
                        }

                        PackUnpack.UnsafeGetAndAdvance<Vector128<byte>>(ref Buf) =
                            PackUnpack.NarrowSixteenUtf16CharsToAscii(v, v2);
                        strPtr += 16;
                    }
                }
            }

            while (strPtr != strPtrEnd)
            {
                var c = *strPtr++;
                if (c < 0x80)
                {
                    if (Buf.IsEmpty)
                    {
                        Resize(1);
                    }

                    PackUnpack.UnsafeGetAndAdvance<byte>(ref Buf) = (byte)c;
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
        TryReserve((uint)l + 1);

        fixed (char* strPtrStart = value)
        {
            var strPtr = strPtrStart;
            var strPtrEnd = strPtrStart + l;
            goFast:
            while (BitConverter.IsLittleEndian && strPtr + 4 <= strPtrEnd && Buf.Length >= 4)
            {
                var c4Data = Unsafe.Read<ulong>(strPtr);
                if (!PackUnpack.AllCharsInUInt64AreAsciiM1(ref c4Data))
                {
                    var c2Data = (uint)c4Data;
                    if (PackUnpack.AllCharsInUInt32AreAsciiM1(ref c2Data))
                    {
                        PackUnpack.UnsafeGetAndAdvance<ushort>(ref Buf) =
                            (ushort)((c2Data >> 8) | c2Data);
                        strPtr += 2;
                        c4Data >>= 32;
                    }

                    if ((c4Data & 0xffff) < 0x7f)
                    {
                        PackUnpack.UnsafeGetAndAdvance<byte>(ref Buf) = (byte)(c4Data + 1);
                        strPtr++;
                    }

                    break;
                }

                PackUnpack.UnsafeGetAndAdvance<uint>(ref Buf) = PackUnpack.NarrowFourUtf16CharsToAscii(c4Data);
                strPtr += 4;

                if ((Sse2.IsSupported || AdvSimd.IsSupported) && strPtr + 8 <= strPtrEnd && Buf.Length >= 8)
                {
                    var v = PackUnpack.Add1Saturate(Unsafe.ReadUnaligned<Vector128<ushort>>(strPtr));
                    if (!PackUnpack.AllCharsInVectorAreAscii(v)) continue;
                    PackUnpack.UnsafeGetAndAdvance<ulong>(ref Buf) = PackUnpack.NarrowEightUtf16CharsToAscii(v);
                    strPtr += 8;
                    while (strPtr + 16 <= strPtrEnd && Buf.Length >= 16)
                    {
                        v = PackUnpack.Add1Saturate(Unsafe.ReadUnaligned<Vector128<ushort>>(strPtr));
                        var v2 = PackUnpack.Add1Saturate(Unsafe.ReadUnaligned<Vector128<ushort>>(strPtr + 8));
                        if (!PackUnpack.AllCharsInVectorAreAscii(v | v2))
                        {
                            if (!PackUnpack.AllCharsInVectorAreAscii(v)) break;
                            PackUnpack.UnsafeGetAndAdvance<ulong>(ref Buf) = PackUnpack.NarrowEightUtf16CharsToAscii(v);
                            strPtr += 8;
                            break;
                        }

                        PackUnpack.UnsafeGetAndAdvance<Vector128<byte>>(ref Buf) =
                            PackUnpack.NarrowSixteenUtf16CharsToAscii(v, v2);
                        strPtr += 16;
                    }
                }
            }

            while (strPtr != strPtrEnd)
            {
                var c = *strPtr++;
                if (c < 0x7f)
                {
                    if (Buf.IsEmpty)
                    {
                        Resize(1);
                    }

                    PackUnpack.UnsafeGetAndAdvance<byte>(ref Buf) = (byte)(c + 1);
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
        var l = Encoding.UTF8.GetByteCount(value);
        WriteVUInt32((uint)l);
        if ((uint)l <= (uint)Buf.Length)
        {
            PackUnpack.UnsafeAdvance(ref Buf, Encoding.UTF8.GetBytes(value.AsSpan(), Buf));
            return;
        }

        Span<byte> buf = l <= 512 ? stackalloc byte[l] : new byte[l];
        Encoding.UTF8.GetBytes(value.AsSpan(), buf);
        WriteBlock(ref MemoryMarshal.GetReference(buf), (uint)buf.Length);
    }

    public void WriteBlock(scoped ReadOnlySpan<byte> data)
    {
        WriteBlock(ref MemoryMarshal.GetReference(data), (uint)data.Length);
    }

    public void WriteBlock(ref byte buffer, uint length)
    {
        if ((uint)Buf.Length < length)
        {
            if (Controller != null)
            {
                if (HeapBuffer != null)
                {
                    var bufLength = HeapBuffer.Length;
                    if (length < bufLength || !Controller.Flush(ref this))
                    {
                        Unsafe.CopyBlockUnaligned(ref MemoryMarshal.GetReference(Buf), ref buffer,
                            (uint)Buf.Length);
                        buffer = ref Unsafe.AddByteOffset(ref buffer, (IntPtr)Buf.Length);
                        length -= (uint)Buf.Length;
                        Buf = new Span<byte>();
                        Controller.Flush(ref this); // must return true because Buf is empty
                    }

                    if (length < bufLength)
                    {
                        Unsafe.CopyBlockUnaligned(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, (int)length),
                            ref buffer, length);
                        return;
                    }
                }

                Controller.WriteBlock(ref this, ref buffer, length);
                return;
            }

            Resize(length); // returns always success because it is without controller
        }

        Unsafe.CopyBlockUnaligned(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, (int)length), ref buffer,
            length);
    }

    public void WriteBlock(byte[] buffer, int offset, int length)
    {
        WriteBlock(buffer.AsSpan(offset, length));
    }

    public unsafe void WriteBlock(IntPtr data, int length)
    {
        WriteBlock(ref Unsafe.AsRef<byte>((void*)data), (uint)length);
    }

    public void WriteBlock(byte[] data)
    {
        WriteBlock(data.AsSpan());
    }

    public void WriteGuid(Guid value)
    {
        WriteBlock(ref Unsafe.As<Guid, byte>(ref Unsafe.AsRef(value)), 16);
    }

    public void WriteSingle(float value)
    {
        WriteInt32(BitConverter.SingleToInt32Bits(value));
    }

    public void WriteDouble(double value)
    {
        WriteInt64(BitConverter.DoubleToInt64Bits(value));
    }

    public void WriteHalf(Half value)
    {
        WriteInt16(Unsafe.As<Half, short>(ref Unsafe.AsRef(value)));
    }

    public void WriteDecimal(decimal value)
    {
        var ints = decimal.GetBits(value);
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
                WriteInt64((long)first);
            }
            else
            {
                header |= 64 | 32;
                WriteUInt8(header);
                WriteInt32(ints[2]);
                WriteInt64((long)first);
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
        if (HeapBuffer != null)
        {
            return (uint)(HeapBuffer.Length - Buf.Length);
        }

        return (uint)(InitialBuffer.Length - Buf.Length);
    }

    public void NoControllerSetCurrentPosition(uint pos)
    {
        if (HeapBuffer != null)
        {
            Buf = HeapBuffer.AsSpan((int)pos);
            return;
        }

        Buf = InitialBuffer[(int)pos..];
    }

    public uint StartWriteByteArray()
    {
        if (Controller != null) ThrowCannotBeUsedWithController();
        WriteByteZero();
        return NoControllerGetCurrentPosition();
    }

    public void FinishWriteByteArray(uint start)
    {
        var end = NoControllerGetCurrentPosition();
        var len = end - start + 1;
        var lenOfLen = PackUnpack.LengthVUInt(len);
        if (lenOfLen == 1)
        {
            if (HeapBuffer != null)
            {
                HeapBuffer[start - 1] = (byte)len;
                return;
            }

            InitialBuffer[(int)(start - 1)] = (byte)len;
            return;
        }

        // Reserve space at end
        TryReserve(lenOfLen - 1);
        PackUnpack.UnsafeAdvance(ref Buf, (int)lenOfLen - 1);
        // Make Space By Moving Memory
        InternalGetSpan(start, len - 1).CopyTo(InternalGetSpan(start + lenOfLen - 1, len - 1));
        // Update Length at start
        PackUnpack.UnsafePackVUInt(ref MemoryMarshal.GetReference(InternalGetSpan(start - 1, lenOfLen)), len, lenOfLen);
    }

    Span<byte> InternalGetSpan(uint start, uint len)
    {
        if (HeapBuffer != null)
        {
            return HeapBuffer.AsSpan((int)start, (int)len);
        }

        return InitialBuffer.Slice((int)start, (int)len);
    }

    public uint StartXor()
    {
        if (Controller != null) ThrowCannotBeUsedWithController();
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
        if (Unsafe.AreSame(ref MemoryMarshal.GetReference(InitialBuffer),
                ref MemoryMarshal.GetReference(writtenBuffer)))
        {
            Buf = InitialBuffer[writtenBuffer.Length..];
            HeapBuffer = null;
        }
        else
        {
            Reset();
            WriteBlock(writtenBuffer);
        }
    }

    public Span<byte> BlockWriteToSpan(int length)
    {
        TryReserve((uint)length);
        return MemoryMarshal.CreateSpan(ref PackUnpack.UnsafeGetAndAdvance(ref Buf, length), length);
    }
}
