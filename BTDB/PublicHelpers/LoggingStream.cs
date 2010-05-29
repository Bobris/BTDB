using System;
using System.Globalization;
using System.Text;

namespace BTDB
{
    class LoggingStream : IStream, IDisposable
    {
        IStream _stream;
        readonly bool _dispose;
        readonly Action<string> _logAction;

        public LoggingStream(IStream stream, bool dispose, Action<string> logAction)
        {
            _stream = stream;
            _dispose = dispose;
            _logAction = logAction;
            Log("LoggingStream created Stream:{0} Dispose:{1}", _stream.ToString(), _dispose);
        }

        private void Log(string what, params object[] parameters)
        {
            _logAction(string.Format(CultureInfo.InvariantCulture, what, parameters));
        }

        public int Read(byte[] data, int offset, int size, ulong pos)
        {
            int res = _stream.Read(data, offset, size, pos);
            Log("read size:{2}/{0} pos:{1} datalen:{3} dataofs:{4}", size, pos, res, data.Length, offset);
            int i = 0;
            int j = 0;
            var sb = new StringBuilder(8 + 16 * 3);
            while (i < res)
            {
                if (j == 16)
                {
                    Log(sb.ToString());
                    sb.Length = 0;
                    j = 0;
                }
                if (j == 0)
                {
                    sb.AppendFormat("{0:X8}", pos + (uint) i);
                }
                sb.AppendFormat(" {0:X2}", data[offset + i]);
                j++;
                i++;
            }
            if (j > 0)
            {
                Log(sb.ToString());
            }
            return res;
        }

        public void Write(byte[] data, int offset, int size, ulong pos)
        {
            Log("write size:{0} pos:{1} datalen:{2} dataofs:{3}", size, pos, data.Length, offset);
            int i = 0;
            int j = 0;
            var sb = new StringBuilder(8 + 16 * 3);
            while (i < size)
            {
                if (j == 16)
                {
                    Log(sb.ToString());
                    sb.Length = 0;
                    j = 0;
                }
                if (j == 0)
                {
                    sb.AppendFormat("{0:X8}", pos + (uint) i);
                }
                sb.AppendFormat(" {0:X2}", data[offset + i]);
                j++;
                i++;
            }
            if (j > 0)
            {
                Log(sb.ToString());
            }
            try
            {
                _stream.Write(data, offset, size, pos);
            }
            catch (Exception ex)
            {
                Log("Exception in write:{0}", ex.ToString());
                throw;
            }
        }

        public void Flush()
        {
            Log("flushing stream");
            _stream.Flush();
        }

        public ulong GetSize()
        {
            ulong res = _stream.GetSize();
            Log("get stream size:{0}", res);
            return res;
        }

        public void SetSize(ulong size)
        {
            Log("setting stream size:{0}", size);
            try
            {
                _stream.SetSize(size);
            }
            catch (Exception ex)
            {
                Log("Exception in setSize:{0}", ex.ToString());
                throw;
            }
        }

        public void Dispose()
        {
            Log("LoggingStream disposed");
            if (_dispose && _stream != null)
            {
                var disp = _stream as IDisposable;
                if (disp != null)
                {
                    disp.Dispose();
                }
                _stream = null;
            }
        }
    }
}