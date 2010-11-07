using System;

namespace BTDB
{
    public class StreamProxy : IStream
    {
        private readonly System.IO.Stream _stream;
        private readonly object _lock = new object();
        private readonly bool _dispose;

        public StreamProxy(string fileName)
        {
            if (string.IsNullOrEmpty(fileName)) throw new ArgumentOutOfRangeException("fileName");
            _stream = new System.IO.FileStream(fileName, System.IO.FileMode.OpenOrCreate);
            _dispose = true;
        }

        public StreamProxy(System.IO.Stream stream, bool dispose)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            _stream = stream;
            _dispose = dispose;
        }

        public int Read(byte[] data, int offset, int size, ulong pos)
        {
            lock (_lock)
            {
                _stream.Position = (long)pos;
                return _stream.Read(data, offset, size);
            }
        }

        public void Write(byte[] data, int offset, int size, ulong pos)
        {
            lock (_lock)
            {
                _stream.Position = (long)pos;
                _stream.Write(data, offset, size);
            }
        }

        public void Flush()
        {
            lock (_lock)
            {
                _stream.Flush();
            }
        }

        public void HardFlush()
        {
            lock (_lock)
            {
                var fileStream = _stream as System.IO.FileStream;
                if (fileStream!=null)
                {
                    fileStream.Flush(true);
                }
                else
                {
                    _stream.Flush();
                }
            }
        }

        public ulong GetSize()
        {
            lock (_lock)
            {
                return (ulong)_stream.Length;
            }
        }

        public void SetSize(ulong size)
        {
            lock (_lock)
            {
                _stream.SetLength((long)size);
            }
        }

        public void Dispose()
        {
            if (_dispose)
            {
                _stream.Dispose();
            }
        }
    }
}