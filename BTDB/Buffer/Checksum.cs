using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.Buffer;

public static class Checksum
{
    public static uint CalcFletcher32(ReadOnlySpan<byte> data)
    {
        var length = (uint)data.Length;
        var odd = (length & 1) != 0;
        length >>= 1;
        ref var position = ref MemoryMarshal.GetReference(data);
        var sum1 = 0xffffu;
        var sum2 = 0xffffu;
        while (length > 0)
        {
            var tlen = length > 360 ? 360 : length;
            length -= tlen;
            do
            {
                sum1 += Unsafe.ReadUnaligned<ushort>(ref position);
                position = ref Unsafe.Add(ref position, 2);
                sum2 += sum1;
            } while (--tlen > 0);

            sum1 = (sum1 & 0xffff) + (sum1 >> 16);
            sum2 = (sum2 & 0xffff) + (sum2 >> 16);
        }

        if (odd)
        {
            sum1 += position;
            sum2 += sum1;
            sum1 = (sum1 & 0xffff) + (sum1 >> 16);
            sum2 = (sum2 & 0xffff) + (sum2 >> 16);
        }

        // Second reduction step to reduce sums to 16 bits
        sum1 = (sum1 & 0xffff) + (sum1 >> 16);
        sum2 = (sum2 & 0xffff) + (sum2 >> 16);
        return sum2 << 16 | sum1;
    }

    public static uint CalcFletcher32(byte[] data, uint position, uint length)
    {
        return CalcFletcher32(data.AsSpan((int)position, (int)length));
    }
}
