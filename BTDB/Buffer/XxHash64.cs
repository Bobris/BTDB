using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.Buffer;

[StructLayout(LayoutKind.Sequential)]
public struct XxHash64
{
    const ulong Prime1 = 11400714785074694791UL;
    const ulong Prime2 = 14029467366897019727UL;
    const ulong Prime3 = 1609587929392839161UL;
    const ulong Prime4 = 9650029242287828579UL;
    const ulong Prime5 = 2870177450012600261UL;

    public XxHash64()
    {
        _v1 = unchecked(Prime1 + Prime2);
        _v2 = Prime2;
        _v3 = 0;
        _v4 = -unchecked((long)Prime1);
    }

    public XxHash64(ulong seed)
    {
        _v1 = seed + Prime1 + Prime2;
        _v2 = seed + Prime2;
        _v3 = seed + 0;
        _v4 = seed - Prime1;
    }

    ulong _v1;
    ulong _v2;
    ulong _v3;
    ulong _v4;

    unsafe fixed byte _buf[32];

    ulong _fullLength = 0;

    public unsafe void Process(ref byte input, uint len)
    {
        if (len == 0) return;
        var bufLen = (uint)_fullLength & 31;
        ref var myBuf = ref Unsafe.AsRef(_buf[0]);
        if (bufLen != 0)
        {
            var copyLen = Math.Min(32u - bufLen, len);
            Unsafe.CopyBlockUnaligned(ref Unsafe.Add(ref myBuf, bufLen),
                ref input, copyLen);
            bufLen += copyLen;
            input = ref Unsafe.Add(ref input, copyLen);
            _fullLength += len;
            if (bufLen != 32) return;
            ProcessMultipliesOf32Bytes(ref myBuf, 32, ref _v1, ref _v2, ref _v3, ref _v4);
            len -= copyLen;
        }
        else
        {
            _fullLength += len;
        }

        if (len >= 32)
        {
            ProcessMultipliesOf32Bytes(ref input, len, ref _v1, ref _v2, ref _v3, ref _v4);
            input = ref Unsafe.Add(ref input, len & ~31u);
            len -= len & ~31u;
        }

        if (len == 0) return;
        Unsafe.CopyBlockUnaligned(ref myBuf, ref input, len);
    }

    public unsafe ulong Finish()
    {
        ref var myBuf = ref Unsafe.AsRef(_buf[0]);
        var bufLen = (uint)_fullLength & 31;
        // _v3 can be used as seed because it is used only when _fullLength < 32
        return Finalize(ref myBuf, bufLen, ref _v1, ref _v2, ref _v3, ref _v4, _fullLength, _v3);
    }

    public static ulong Hash(ref byte input, uint len, ulong seed)
    {
        ulong h64;

        if (len >= 32)
        {
            var repetitions = len / 32;

            var v1 = seed + Prime1 + Prime2;
            var v2 = seed + Prime2;
            var v3 = seed + 0;
            var v4 = seed - Prime1;

            do
            {
                v1 = Round(v1, Unsafe.AsRef<ulong>(input));
                input = ref Unsafe.Add(ref input, 8);
                v2 = Round(v2, Unsafe.AsRef<ulong>(input));
                input = ref Unsafe.Add(ref input, 8);
                v3 = Round(v3, Unsafe.AsRef<ulong>(input));
                input = ref Unsafe.Add(ref input, 8);
                v4 = Round(v4, Unsafe.AsRef<ulong>(input));
                input = ref Unsafe.Add(ref input, 8);
            } while (repetitions-- > 0);

            h64 = BitOperations.RotateLeft(v1, 1) +
                  BitOperations.RotateLeft(v2, 7) +
                  BitOperations.RotateLeft(v3, 12) +
                  BitOperations.RotateLeft(v4, 18);

            h64 = MergeRound(h64, v1);
            h64 = MergeRound(h64, v2);
            h64 = MergeRound(h64, v3);
            h64 = MergeRound(h64, v4);
        }
        else
        {
            h64 = seed + Prime5;
        }

        h64 += len;

        return Finalize(h64, ref input, len);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ulong Round(ulong acc, ulong input)
    {
        acc += input * Prime2;
        acc = BitOperations.RotateLeft(acc, 31);
        acc *= Prime1;
        return acc;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ulong MergeRound(ulong acc, ulong val)
    {
        val = Round(0, val);
        acc ^= val;
        acc = acc * Prime1 + Prime4;
        return acc;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ulong Avalanche(ulong hash)
    {
        hash ^= hash >> 33;
        hash *= Prime2;
        hash ^= hash >> 29;
        hash *= Prime3;
        hash ^= hash >> 32;
        return hash;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ulong Finalize(ulong hash, ref byte ptr, uint len)
    {
        len &= 31;
        while (len >= 8)
        {
            var k1 = Round(0, Unsafe.AsRef<ulong>(ptr));
            ptr = ref Unsafe.Add(ref ptr, 8);
            hash ^= k1;
            hash = BitOperations.RotateLeft(hash, 27) * Prime1 + Prime4;
            len -= 8;
        }

        if (len >= 4)
        {
            hash ^= Unsafe.AsRef<uint>(ptr) * Prime1;
            ptr = ref Unsafe.Add(ref ptr, 4);
            hash = BitOperations.RotateLeft(hash, 23) * Prime2 + Prime3;
            len -= 4;
        }

        while (len > 0)
        {
            hash ^= ptr * Prime5;
            ptr = ref Unsafe.Add(ref ptr, 1);
            hash = BitOperations.RotateLeft(hash, 11) * Prime1;
            --len;
        }

        return Avalanche(hash);
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void ProcessMultipliesOf32Bytes(ref byte input, uint len, ref ulong v1, ref ulong v2, ref ulong v3,
        ref ulong v4)
    {
        var repetitions = len / 32;
        do
        {
            var reg1 = Unsafe.ReadUnaligned<ulong>(ref input);
            input = ref Unsafe.Add(ref input, 8);
            var reg2 = Unsafe.ReadUnaligned<ulong>(ref input);
            input = ref Unsafe.Add(ref input, 8);
            var reg3 = Unsafe.ReadUnaligned<ulong>(ref input);
            input = ref Unsafe.Add(ref input, 8);
            var reg4 = Unsafe.ReadUnaligned<ulong>(ref input);
            input = ref Unsafe.Add(ref input, 8);

            v1 = Round(v1, reg1);
            v2 = Round(v2, reg2);
            v3 = Round(v3, reg3);
            v4 = Round(v4, reg4);
        } while (repetitions-- > 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ulong Finalize(ref byte input, uint len, ref ulong v1, ref ulong v2, ref ulong v3,
        ref ulong v4, ulong fullLength, ulong seed)
    {
        ulong h64;

        if (fullLength >= 32)
        {
            h64 = ((v1 << 1) | (v1 >> (64 - 1))) +
                  ((v2 << 7) | (v2 >> (64 - 7))) +
                  ((v3 << 12) | (v3 >> (64 - 12))) +
                  ((v4 << 18) | (v4 >> (64 - 18)));

            h64 = MergeRound(h64, v1);
            h64 = MergeRound(h64, v2);
            h64 = MergeRound(h64, v3);
            h64 = MergeRound(h64, v4);
        }
        else
        {
            h64 = seed + Prime5;
        }

        h64 += fullLength;

        return Finalize(h64, ref input, len);
    }
}
