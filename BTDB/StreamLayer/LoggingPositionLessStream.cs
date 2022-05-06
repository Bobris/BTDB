using System;
using System.Globalization;
using System.Text;

namespace BTDB.StreamLayer;

public class LoggingPositionLessStream : IPositionLessStream
{
    IPositionLessStream _positionLessStream;
    readonly bool _dispose;
    readonly Action<string> _logAction;

    public LoggingPositionLessStream(IPositionLessStream positionLessStream, bool dispose, Action<string> logAction)
    {
        _positionLessStream = positionLessStream;
        _dispose = dispose;
        _logAction = logAction;
        Log("LoggingStream created Stream:{0} Dispose:{1}", _positionLessStream.ToString(), _dispose);
    }

    void Log(string what, params object[] parameters)
    {
        _logAction(string.Format(CultureInfo.InvariantCulture, what, parameters));
    }

    public int Read(Span<byte> data, ulong pos)
    {
        int res = _positionLessStream.Read(data, pos);
        Log("read size:{2}/{0} pos:{1}", data.Length, pos, res);
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
                sb.AppendFormat("{0:X8}", pos + (uint)i);
            }
            sb.AppendFormat(" {0:X2}", data[i]);
            j++;
            i++;
        }
        if (j > 0)
        {
            Log(sb.ToString());
        }
        return res;
    }

    public void Write(ReadOnlySpan<byte> data, ulong pos)
    {
        Log("write size:{0} pos:{1} datalen:{2}", data.Length, pos, data.Length);
        int i = 0;
        int j = 0;
        var sb = new StringBuilder(8 + 16 * 3);
        while (i < data.Length)
        {
            if (j == 16)
            {
                Log(sb.ToString());
                sb.Length = 0;
                j = 0;
            }
            if (j == 0)
            {
                sb.AppendFormat("{0:X8}", pos + (uint)i);
            }
            sb.AppendFormat(" {0:X2}", data[i]);
            j++;
            i++;
        }
        if (j > 0)
        {
            Log(sb.ToString());
        }
        try
        {
            _positionLessStream.Write(data, pos);
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
        _positionLessStream.Flush();
    }

    public void HardFlush()
    {
        Log("hard flushing stream");
        _positionLessStream.HardFlush();
    }

    public ulong GetSize()
    {
        ulong res = _positionLessStream.GetSize();
        Log("get stream size:{0}", res);
        return res;
    }

    public void SetSize(ulong size)
    {
        Log("setting stream size:{0}", size);
        try
        {
            _positionLessStream.SetSize(size);
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
        if (_dispose && _positionLessStream != null)
        {
            _positionLessStream.Dispose();
            _positionLessStream = null;
        }
    }
}
