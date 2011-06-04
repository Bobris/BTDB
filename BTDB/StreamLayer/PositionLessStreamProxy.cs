using System;
using System.IO;

namespace BTDB.StreamLayer
{
    public class PositionLessStreamProxy : IPositionLessStream
    {
        readonly Stream _stream;
        readonly object _lock = new object();
        readonly bool _dispose;
        readonly int _writeBufSize;
        ulong _position;
        ulong _writeBufStart;
        int _writeBufUsed;
        byte[] _writeBuf;

        public PositionLessStreamProxy(string fileName)
        {
            if (string.IsNullOrEmpty(fileName)) throw new ArgumentOutOfRangeException("fileName");
            _stream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1, FileOptions.RandomAccess);
            _dispose = true;
            _position = 0;
            _writeBufUsed = 0;
            _writeBufSize = 32768;
        }

        public PositionLessStreamProxy(Stream stream, bool dispose)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            _stream = stream;
            _position = (ulong)_stream.Position;
            _dispose = dispose;
            _writeBufUsed = 0;
        }

        public int Read(byte[] data, int offset, int size, ulong pos)
        {
            lock (_lock)
            {
                FlushWriteBuf();
                if (_position != pos)
                {
                    _stream.Position = (long)pos;
                    _position = pos;
                }
                try
                {
                    var res = _stream.Read(data, offset, size);
                    _position += (ulong)res;
                    return res;
                }
                catch (Exception)
                {
                    _position = (ulong)_stream.Position;
                    throw;
                }
            }
        }

        void FlushWriteBuf()
        {
            if (_writeBufUsed == 0) return;
            if (_position != _writeBufStart)
            {
                _stream.Position = (long)_writeBufStart;
                _position = _writeBufStart;
            }
            try
            {
                _stream.Write(_writeBuf, 0, _writeBufUsed);
                _position += (ulong)_writeBufUsed;
                _writeBufUsed = 0;
            }
            catch (Exception)
            {
                _position = (ulong)_stream.Position;
                _writeBufUsed = 0;
                throw;
            }
        }

        public void Write(byte[] data, int offset, int size, ulong pos)
        {
            lock (_lock)
            {
                if (size < _writeBufSize)
                {
                    if (_writeBuf == null)
                    {
                        _writeBuf = new byte[_writeBufSize];
                    }
                    if (_writeBufUsed > 0)
                    {
                        if (pos >= _writeBufStart && pos <= _writeBufStart + (ulong) _writeBufUsed &&
                            pos + (ulong) size <= _writeBufStart + (ulong) _writeBufSize)
                        {
                            var writeBufOfs = (int) (pos - _writeBufStart);
                            Array.Copy(data, offset, _writeBuf, writeBufOfs, size);
                            _writeBufUsed = Math.Max(_writeBufUsed, writeBufOfs + size);
                            return;
                        }
                    }
                    else
                    {
                        _writeBufStart = pos;
                        Array.Copy(data, offset, _writeBuf, 0, size);
                        _writeBufUsed = size;
                        return;
                    }
                }
                FlushWriteBuf();
                if (_position != pos)
                {
                    _stream.Position = (long)pos;
                    _position = pos;
                }
                try
                {
                    _stream.Write(data, offset, size);
                    _position += (ulong)size;
                }
                catch (Exception)
                {
                    _position = (ulong)_stream.Position;
                    throw;
                }
            }
        }

        public void Flush()
        {
            lock (_lock)
            {
                FlushWriteBuf();
                _stream.Flush();
            }
        }

        public void HardFlush()
        {
            lock (_lock)
            {
                FlushWriteBuf();
                var fileStream = _stream as FileStream;
                if (fileStream != null)
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
                var res = (ulong)_stream.Length;
                if (_writeBufUsed > 0)
                {
                    res = Math.Max(res, _writeBufStart + (ulong)_writeBufUsed);
                }
                return res;
            }
        }

        public void SetSize(ulong size)
        {
            lock (_lock)
            {
                FlushWriteBuf();
                _stream.SetLength((long)size);
                _position = (ulong)_stream.Position;
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