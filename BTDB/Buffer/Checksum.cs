namespace BTDB.Buffer;

public static class Checksum
{
    public static uint CalcFletcher32(byte[] data, uint position, uint length)
    {
        var odd = (length & 1) != 0;
        length >>= 1;
        uint sum1 = 0xffff;
        uint sum2 = 0xffff;
        while (length > 0)
        {
            uint tlen = length > 360 ? 360 : length;
            length -= tlen;
            do
            {
                sum1 += (uint)(data[position] + data[position + 1] * 256);
                position += 2;
                sum2 += sum1;
            }
            while (--tlen > 0);
            sum1 = (sum1 & 0xffff) + (sum1 >> 16);
            sum2 = (sum2 & 0xffff) + (sum2 >> 16);
        }
        if (odd)
        {
            sum1 += data[position];
            sum2 += sum1;
            sum1 = (sum1 & 0xffff) + (sum1 >> 16);
            sum2 = (sum2 & 0xffff) + (sum2 >> 16);
        }
        // Second reduction step to reduce sums to 16 bits
        sum1 = (sum1 & 0xffff) + (sum1 >> 16);
        sum2 = (sum2 & 0xffff) + (sum2 >> 16);
        return sum2 << 16 | sum1;
    }
}
