namespace BTDB.StreamLayer;

public interface IMemorizedPosition
{
    void Restore(ref SpanReader reader);
}

public class MemorizedPosition : IMemorizedPosition
{
    readonly long _pos;

    public MemorizedPosition(long pos)
    {
        _pos = pos;
    }

    public void Restore(ref SpanReader reader)
    {
        reader.SetCurrentPosition(_pos);
    }
}
