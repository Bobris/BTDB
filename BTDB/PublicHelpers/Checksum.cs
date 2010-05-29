using System.Diagnostics;

namespace BTDB
{
    public static class Checksum
    {
        public static uint CalcFletcher(byte[] data, uint position, uint length)
        {
            Debug.Assert((length & 1) == 0);
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

            // Second reduction step to reduce sums to 16 bits
            sum1 = (sum1 & 0xffff) + (sum1 >> 16);
            sum2 = (sum2 & 0xffff) + (sum2 >> 16);
            return sum2 << 16 | sum1;
        }
    }
}