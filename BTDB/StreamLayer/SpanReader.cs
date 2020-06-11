using System;
using System.IO;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;

namespace BTDB.StreamLayer
{
    public ref struct SpanReader
    {
        public SpanReader(in ReadOnlySpan<byte> data)
        {
            _buf = data;
            _original = data;
            _controller = null;
        }

        public SpanReader(ISpanReader controller)
        {
            _controller = controller;
            _buf = new ReadOnlySpan<byte>();
            _original = new ReadOnlySpan<byte>();
            controller.Init(ref this);
        }

        ReadOnlySpan<byte> _buf;
        ReadOnlySpan<byte> _original;
        ISpanReader? _controller;

        public long GetCurrentPosition()
        {
            return _controller?.GetCurrentPosition(this) ?? (long) Unsafe.ByteOffset(ref MemoryMarshal.GetReference(_original),
                ref MemoryMarshal.GetReference(_buf));
        }

        public bool Eof
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _buf.IsEmpty && (_controller?.FillBufAndCheckForEof(ref this) ?? true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void NeedOneByteInBuffer()
        {
            if (Eof)
            {
                PackUnpack.ThrowEndOfStreamException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void RequireBufLength(int len)
        {
            if (_buf.Length < len && (_controller?.FillBufAndCheckForEof(ref this, len) ?? true))
                PackUnpack.ThrowEndOfStreamException();
        }

        public bool ReadBool()
        {
            NeedOneByteInBuffer();
            return PackUnpack.UnsafeGetAndAdvance(ref _buf, 1) != 0;
        }

        public void SkipBool()
        {
            SkipUInt8();
        }

        public byte ReadUInt8()
        {
            NeedOneByteInBuffer();
            return PackUnpack.UnsafeGetAndAdvance(ref _buf, 1);
        }

        public void SkipUInt8()
        {
            NeedOneByteInBuffer();
            PackUnpack.UnsafeAdvance(ref _buf, 1);
        }

        public sbyte ReadInt8()
        {
            NeedOneByteInBuffer();
            return (sbyte) PackUnpack.UnsafeGetAndAdvance(ref _buf, 1);
        }

        public void SkipInt8()
        {
            SkipUInt8();
        }

        public sbyte ReadInt8Ordered()
        {
            NeedOneByteInBuffer();
            return (sbyte) (PackUnpack.UnsafeGetAndAdvance(ref _buf, 1) - 128);
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
            return (short) res;
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
            return (ushort) res;
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
            return (int) res;
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
            return (uint) res;
        }

        public void SkipVUInt32()
        {
            var res = ReadVUInt64();
            if (res > uint.MaxValue) throw new InvalidDataException($"Skipping VUInt32 overflowed with {res}");
        }

        public long ReadVInt64()
        {
            NeedOneByteInBuffer();
            ref var byteRef = ref MemoryMarshal.GetReference(_buf);
            var len = PackUnpack.LengthVIntByFirstByte(byteRef);
            RequireBufLength(len);
            PackUnpack.UnsafeAdvance(ref _buf, len);
            return PackUnpack.UnsafeUnpackVInt(ref byteRef, len);
        }

        public void SkipVInt64()
        {
            NeedOneByteInBuffer();
            var len = PackUnpack.LengthVIntByFirstByte(MemoryMarshal.GetReference(_buf));
            RequireBufLength(len);
            PackUnpack.UnsafeAdvance(ref _buf, len);
        }

        public ulong ReadVUInt64()
        {
            NeedOneByteInBuffer();
            ref var byteRef = ref MemoryMarshal.GetReference(_buf);
            var len = PackUnpack.LengthVUIntByFirstByte(byteRef);
            RequireBufLength(len);
            PackUnpack.UnsafeAdvance(ref _buf, len);
            return PackUnpack.UnsafeUnpackVUInt(ref byteRef, len);
        }

        public void SkipVUInt64()
        {
            NeedOneByteInBuffer();
            ref var byteRef = ref MemoryMarshal.GetReference(_buf);
            var len = PackUnpack.LengthVUIntByFirstByte(byteRef);
            RequireBufLength(len);
            PackUnpack.UnsafeAdvance(ref _buf, len);
        }

        public long ReadInt64()
        {
            RequireBufLength(8);
            return (long) PackUnpack.AsBigEndian(Unsafe.As<byte, ulong>(ref PackUnpack.UnsafeGetAndAdvance(ref _buf, 8)));
        }

        public void SkipInt64()
        {
            RequireBufLength(8);
            PackUnpack.UnsafeAdvance(ref _buf, 8);
        }

        public int ReadInt32()
        {
            RequireBufLength(4);
            return (int) PackUnpack.AsBigEndian(Unsafe.As<byte, uint>(ref PackUnpack.UnsafeGetAndAdvance(ref _buf, 4)));
        }

        public int ReadInt32LE()
        {
            RequireBufLength(4);
            return (int) PackUnpack.AsLittleEndian(Unsafe.As<byte, uint>(ref PackUnpack.UnsafeGetAndAdvance(ref _buf, 4)));
        }

        public void SkipInt32()
        {
            RequireBufLength(4);
            PackUnpack.UnsafeAdvance(ref _buf, 4);
        }

        public DateTime ReadDateTime()
        {
            return DateTime.FromBinary(ReadInt64());
        }

        public void SkipDateTime()
        {
            SkipInt64();
        }

        public TimeSpan ReadTimeSpan()
        {
            return new TimeSpan(ReadVInt64());
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
            var l = (int) len;
            if (l == 0) return "";
            var result = new string('\0', l);
            fixed (char* res = result)
            {
                var i = 0;
                while (i < l)
                {
                    NeedOneByteInBuffer();
                    var cc = MemoryMarshal.GetReference(_buf);
                    if (cc < 0x80)
                    {
                        res[i++] = (char) cc;
                        PackUnpack.UnsafeAdvance(ref _buf, 1);
                        continue;
                    }

                    var c = ReadVUInt64();
                    if (c > 0xffff)
                    {
                        if (c > 0x10ffff)
                            throw new InvalidDataException($"Reading String unicode value overflowed with {c}");
                        c -= 0x10000;
                        res[i++] = (char) ((c >> 10) + 0xD800);
                        res[i++] = (char) ((c & 0x3FF) + 0xDC00);
                    }
                    else
                    {
                        res[i++] = (char) c;
                    }
                }
            }

            return result;
        }

        public string? ReadStringOrdered()
        {
            var len = 0;
            Span<char> charStackBuf = stackalloc char[32];
            var charBuf = charStackBuf;

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
                    Unsafe.Add(ref MemoryMarshal.GetReference(charBuf), len++) = (char) ((c >> 10) + 0xD800);
                    Unsafe.Add(ref MemoryMarshal.GetReference(charBuf), len++) = (char) ((c & 0x3FF) + 0xDC00);
                }
                else
                {
                    if (charBuf.Length < len + 1)
                    {
                        var newCharBuf = (Span<char>)new char[charBuf.Length * 2];
                        charBuf.CopyTo(newCharBuf);
                        charBuf = newCharBuf;
                    }
                    Unsafe.Add(ref MemoryMarshal.GetReference(charBuf), len++) = (char) c;
                }
            }

            return len == 0 ? "" : new string(charBuf.Slice(0, len));
        }

        public void SkipString()
        {
            var len = ReadVUInt64();
            if (len == 0) return;
            len--;
            if (len > int.MaxValue) throw new InvalidDataException($"Skipping String length overflowed with {len}");
            var l = (int) len;
            if (l == 0) return;
            var i = 0;
            while (i < l)
            {
                var c = ReadVUInt64();
                if (c > 0xffff)
                {
                    if (c > 0x10ffff)
                        throw new InvalidDataException(
                            $"Skipping String unicode value overflowed with {c}");
                    i += 2;
                }
                else
                {
                    i++;
                }
            }
        }

        public void SkipStringOrdered()
        {
            var c = ReadVUInt32();
            if (c == 0) return;
            c--;
            if (c > 0x10ffff)
            {
                if (c == 0x110000) return;
                throw new InvalidDataException($"Skipping String unicode value overflowed with {c}");
            }

            while (true)
            {
                c = ReadVUInt32();
                if (c == 0) break;
                c--;
                if (c > 0x10ffff) throw new InvalidDataException($"Skipping String unicode value overflowed with {c}");
            }
        }

        public void ReadBlock(ref byte buffer, int length)
        {
            if (length > _buf.Length)
            {
                if (_controller?.ReadBlock(ref this, ref buffer, length) ?? true)
                    PackUnpack.ThrowEndOfStreamException();
            }
            Unsafe.CopyBlock(ref buffer, ref PackUnpack.UnsafeGetAndAdvance(ref _buf, length), (uint)length);
        }

        public void ReadBlock(in Span<byte> buffer)
        {
            ReadBlock(ref MemoryMarshal.GetReference(buffer), buffer.Length);
        }

        public void ReadBlock(byte[] data, int offset, int length)
        {
            ReadBlock(data.AsSpan(offset, length));
        }

        public void SkipBlock(int length)
        {
            if (length > _buf.Length)
            {
                if (_controller?.SkipBlock(ref this, length) ?? true)
                    PackUnpack.ThrowEndOfStreamException();
            }
            PackUnpack.UnsafeAdvance(ref _buf, length);
        }

        public void SkipBlock(uint length)
        {
            while (length > int.MaxValue)
            {
                SkipBlock(int.MaxValue);
                length -= int.MaxValue;
            }

            SkipBlock((int) length);
        }

        public void ReadBlock(ByteBuffer buffer)
        {
            ReadBlock(buffer.AsSyncSpan());
        }

        public Guid ReadGuid()
        {
            Span<byte> buffer = stackalloc byte[16];
            ReadBlock(ref MemoryMarshal.GetReference(buffer), 16);
            return new Guid(buffer);
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

        public void SkipDouble()
        {
            SkipInt64();
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
                    first = (ulong) ReadInt64();
                    break;
                case 3:
                    second = (uint) ReadInt32();
                    first = (ulong) ReadInt64();
                    break;
            }

            var res = new decimal((int) first, (int) (first >> 32), (int) second, (header & 128) != 0,
                (byte) (header & 31));
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

        public byte[] ReadByteArrayRawTillEof()
        {
            var res = _buf.ToArray();
            PackUnpack.UnsafeAdvance(ref _buf, _buf.Length);
            return res;
        }

        public byte[] ReadByteArrayRaw(int len)
        {
            var res = new byte[len];
            ReadBlock(res);
            return res;
        }

        public bool CheckMagic(byte[] magic)
        {
            if (_buf.Length < magic.Length) return false;
            if (!_buf.Slice(0, magic.Length).SequenceEqual(magic)) return false;
            PackUnpack.UnsafeAdvance(ref _buf, magic.Length);
            return true;
        }

        public IPAddress? ReadIPAddress()
        {
            switch (ReadUInt8())
            {
                case 0:
                    return new IPAddress((uint) ReadInt32LE());
                case 1:
                {
                    Span<byte> ip6Bytes = stackalloc byte[16];
                    ReadBlock(ref MemoryMarshal.GetReference(ip6Bytes), 16);
                    return new IPAddress(ip6Bytes);
                }
                case 2:
                {
                    Span<byte> ip6Bytes = stackalloc byte[16];
                    ReadBlock(ref MemoryMarshal.GetReference(ip6Bytes), 16);
                    var scopeid = (long) ReadVUInt64();
                    return new IPAddress(ip6Bytes, scopeid);
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
            if (minor == 0) return new Version((int) major - 1, 0);
            var build = ReadVUInt32();
            if (build == 0) return new Version((int) major - 1, (int) minor - 1);
            var revision = ReadVUInt32();
            if (revision == 0) return new Version((int) major - 1, (int) minor - 1, (int) build - 1);
            return new Version((int) major - 1, (int) minor - 1, (int) build - 1, (int) revision - 1);
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
    }
}
