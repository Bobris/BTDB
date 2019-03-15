using System;

namespace ARTLibTest
{
    public class CursorTestsAny : CursorTestsBase
    {
        public override bool Is12 => false;

        byte[] _value;

        public override ReadOnlySpan<byte> GetSampleValue(int index = 0)
        {
            int len;
            if (index == 0) len = 100;
            else len = (index - 1) % 10;
            _value = new byte[len];
            for (int i = 0; i < len; i++)
            {
                _value[i] = (byte)(index + i);
            }
            return _value;
        }
    }
}
