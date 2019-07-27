using System;

namespace ARTLibTest
{
    class CursorTests12 : CursorTestsBase
    {
        public override bool Is12 => true;

        byte[] _key = new byte[12];

        public override ReadOnlySpan<byte> GetSampleValue(int index = 0)
        {
            for (int i = 0; i < 12; i++)
            {
                _key[i] = (byte)(index + i);
            }
            return _key;
        }
    }
}
