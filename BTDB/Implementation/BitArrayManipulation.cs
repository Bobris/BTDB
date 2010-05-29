using System;
using System.Diagnostics;

namespace BTDB
{
    internal static class BitArrayManipulation
    {
        static readonly byte[] FirstHoleSize = new byte[]
                                                           {
                                                               8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0,
                                                               2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1,
                                                               0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0,
                                                               1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2,
                                                               0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                                                               4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1,
                                                               0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0,
                                                               1, 0, 3, 0, 1, 0, 2, 0, 1, 0
                                                           };

        static readonly byte[] LastHoleSize = new byte[]
                                                          {
                                                              8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2,
                                                              2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1,
                                                              1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                                              1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                              0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                                                          };

        static readonly byte[] MaxHoleSize = new byte[]
                                                         {
                                                             8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 5, 4, 3, 3,
                                                             2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 6, 5, 4, 4, 3, 3, 3,
                                                             3, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2,
                                                             1, 1, 2, 1, 1, 1, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 7, 6, 5, 5, 4, 4, 4, 4, 3, 3, 3, 3, 3,
                                                             3, 3, 3, 4, 3, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 1,
                                                             4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 6, 5, 4, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2,
                                                             2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 1, 4, 3, 2, 2, 2, 1,
                                                             1, 1, 3, 2, 1, 1, 2, 1, 1, 0
                                                         };

        static readonly byte[] MaxHoleOffset = new byte[]
                                                           {
                                                               0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 0, 1, 5, 5, 5, 5, 5, 5, 0, 5, 5, 5, 5, 5, 5, 5, 0, 1, 2, 2,
                                                               0, 3, 3, 3, 0, 1, 6, 6, 0, 6, 6, 6, 0, 1, 2, 2, 0, 6, 6, 6, 0, 1, 6, 6, 0, 6, 6, 6, 0, 1, 2, 2, 3, 3, 3,
                                                               3, 0, 1, 4, 4, 0, 4, 4, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1,
                                                               0, 2, 0, 1, 0, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 2, 2, 3, 3, 3, 3, 0, 4, 4, 4, 4,
                                                               4, 4, 4, 0, 1, 2, 2, 0, 5, 5, 5, 0, 1, 5, 5, 0, 5, 5, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 0, 2, 0, 1, 0, 4,
                                                               0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 2, 2, 3, 3, 3, 3, 0, 1, 4, 4, 0, 4, 4, 4, 0, 1, 2,
                                                               2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 2, 2, 0, 1,
                                                               0, 3, 0, 1, 0, 2, 0, 1, 0, 0
                                                           };

        static int IndexOfFirstHole(byte[] data, int size)
        {
            int pos = 0;
            int sizetill = 0;
            int laststart = 0;
            while (pos < data.Length)
            {
                byte b = data[pos];
                pos++;
                switch (b)
                {
                    case 255:
                        if (sizetill >= size) return laststart;
                        sizetill = 0;
                        laststart = pos * 8;
                        break;
                    case 0:
                        sizetill += 8;
                        break;
                    default:
                        sizetill += FirstHoleSize[b];
                        if (sizetill >= size) return laststart;
                        if (MaxHoleSize[b] >= size) return pos * 8 + MaxHoleOffset[b] - 8;
                        sizetill = LastHoleSize[b];
                        laststart = pos * 8 - sizetill;
                        break;
                }
            }
            if (sizetill >= size) return laststart;
            return -1;
        }

        internal static void SetBits(byte[] data, int position, int size)
        {
            Debug.Assert(position >= 0 && size > 0 && position + size <= data.Length * 8);
            var startMask = (byte)~(255 >> (8 - (position & 7)));
            int startBytePos = position / 8;
            var endMask = (byte)(255 >> (7 - ((position + size - 1) & 7)));
            int endBytePos = (position + size - 1) / 8;
            if (startBytePos == endBytePos)
            {
                data[startBytePos] |= (byte)(startMask & endMask);
            }
            else
            {
                data[startBytePos] |= startMask;
                startBytePos++;
                while (startBytePos < endBytePos)
                {
                    data[startBytePos] = 255;
                    startBytePos++;
                }
                data[endBytePos] |= endMask;
            }
        }

        internal static void UnsetBits(byte[] data, int position, int size)
        {
            Debug.Assert(position >= 0 && size > 0 && position + size <= data.Length * 8);
            var startMask = (byte)~(255 >> (8 - (position & 7)));
            int startBytePos = position / 8;
            var endMask = (byte)(255 >> (7 - ((position + size - 1) & 7)));
            int endBytePos = (position + size - 1) / 8;
            if (startBytePos == endBytePos)
            {
                data[startBytePos] &= (byte)~(startMask & endMask);
            }
            else
            {
                data[startBytePos] &= (byte)~startMask;
                startBytePos++;
                while (startBytePos < endBytePos)
                {
                    data[startBytePos] = 0;
                    startBytePos++;
                }
                data[endBytePos] &= (byte)~endMask;
            }
        }

        internal static int SizeOfBiggestHoleUpTo255(byte[] data)
        {
            int pos = 0;
            int sizetill = 0;
            int sizemax = 0;
            while (pos < data.Length)
            {
                byte b = data[pos];
                pos++;
                if (b == 255)
                {
                    if (sizetill > sizemax) sizemax = sizetill;
                    sizetill = 0;
                }
                else if (b == 0)
                {
                    sizetill += 8;
                    if (sizetill > 255) break;
                }
                else
                {
                    sizetill += FirstHoleSize[b];
                    if (sizetill > sizemax) sizemax = sizetill;
                    if (MaxHoleSize[b] > sizemax) sizemax = MaxHoleSize[b];
                    sizetill = LastHoleSize[b];
                }
            }
            if (sizetill > sizemax) sizemax = sizetill;
            if (sizemax > 255) sizemax = 255;
            return sizemax;
        }

        internal static int CompareByteArray(byte[] a1, int o1, int l1, byte[] a2, int o2, int l2)
        {
            int commonLength = Math.Min(l1, l2);
            for (int i = 0; i < commonLength; i++)
            {
                if (a1[o1 + i] < a2[o2 + i]) return -1;
                if (a1[o1 + i] > a2[o2 + i]) return 1;
            }
            if (l1 < l2) return -1;
            if (l1 > l2) return 1;
            return 0;
        }
    }
}