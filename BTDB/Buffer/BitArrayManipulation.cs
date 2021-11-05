using System;

namespace BTDB.Buffer;

static class BitArrayManipulation
{
    internal static int CompareByteArray(byte[] a1, int l1, byte[] a2, int l2)
    {
        var commonLength = Math.Min(l1, l2);
        for (var i = 0; i < commonLength; i++)
        {
            var b1 = a1[i];
            var b2 = a2[i];
            if (b1 == b2) continue;
            if (b1 < b2) return -1;
            return 1;
        }
        if (l1 == l2) return 0;
        if (l1 < l2) return -1;
        return 1;
    }

    internal static int CompareByteArray(byte[] a1, int o1, int l1, byte[] a2, int o2, int l2)
    {
        var commonLength = Math.Min(l1, l2);
        for (var i = 0; i < commonLength; i++)
        {
            var b1 = a1[o1 + i];
            var b2 = a2[o2 + i];
            if (b1 == b2) continue;
            if (b1 < b2) return -1;
            return 1;
        }
        if (l1 == l2) return 0;
        if (l1 < l2) return -1;
        return 1;
    }
}
