using BTDB.Buffer;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace BTDBTest
{
    public class ByteBufferTests
    {
        [Fact]
        public void ConversionFromReadOnlyMemoryWorks()
        {
            var b = new byte[10];
            var m = b.AsMemory(3, 5);
            var bb = ByteBuffer.NewAsync(m);
            Assert.Equal(b, bb.Buffer);
            Assert.Equal(3, bb.Offset);
            Assert.Equal(5, bb.Length);
        }
    }
}
