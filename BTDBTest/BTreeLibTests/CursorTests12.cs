using System;

namespace BTDBTest.BTreeLibTests;

public class CursorTests12 : CursorTestsBase
{
    readonly byte[] _key = new byte[12];

    protected override ReadOnlySpan<byte> GetSampleValue(int index = 0)
    {
        for (var i = 0; i < 12; i++)
        {
            _key[i] = (byte)(index + i);
        }
        return _key;
    }
}
