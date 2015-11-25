using System.Runtime.InteropServices;

namespace BTDB.Buffer
{
    [StructLayout(LayoutKind.Explicit)]
    public struct Int32SingleUnion
    {
        [FieldOffset(0)] readonly int _i;
        [FieldOffset(0)] readonly float _f;

        public Int32SingleUnion(int i)
        {
            _f = 0;
            _i = i;
        }

        public Int32SingleUnion(float f)
        {
            _i = 0;
            _f = f;
        }

        public int AsInt32 => _i;

        public float AsSingle => _f;
    }
}