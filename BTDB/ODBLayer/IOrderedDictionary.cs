using System.Collections.Generic;

namespace BTDB.ODBLayer;

public interface IOrderedDictionaryEnumerator<TKey, TValue>
{
    /// <summary>
    /// Read current cursor position key and move cursor to next pair, if at End just return false and stay at End
    /// </summary>
    /// <param name="key">Filled by read key</param>
    /// <returns>true if there was new key read</returns>
    bool NextKey(out TKey key);
    uint Position { get; set; }
    uint Count { get; }
    TValue CurrentValue { get; set; }
}

public enum KeyProposition
{
    Ignored,
    Included,
    Excluded
}

public enum EnumerationOrder
{
    Ascending,
    Descending
}

internal enum SeekState
{
    Undefined, // position in enumerator is not defined
    SeekNeeded, // position is defined, but seek was not done yet
    Ready,
}

public class AdvancedEnumeratorParam<TKey>
{
    public AdvancedEnumeratorParam()
    {
        Order = EnumerationOrder.Ascending;
        StartProposition = KeyProposition.Ignored;
        EndProposition = KeyProposition.Ignored;
    }

    public AdvancedEnumeratorParam(EnumerationOrder order)
    {
        Order = order;
        StartProposition = KeyProposition.Ignored;
        EndProposition = KeyProposition.Ignored;
    }

    public AdvancedEnumeratorParam(EnumerationOrder order, TKey start, KeyProposition startProposition, TKey end, KeyProposition endProposition)
    {
        Order = order;
        Start = start;
        StartProposition = startProposition;
        End = end;
        EndProposition = endProposition;
    }

    public readonly EnumerationOrder Order;
    public readonly KeyProposition StartProposition;
    public readonly TKey Start;
    public readonly KeyProposition EndProposition;
    public readonly TKey End;

    static volatile AdvancedEnumeratorParam<TKey> _instance;
    static object _syncRoot = new object();

    public static AdvancedEnumeratorParam<TKey> Instance
    {
        get
        {
            if (_instance == null)
            {
                lock (_syncRoot)
                {
                    if (_instance == null)
                        _instance = new AdvancedEnumeratorParam<TKey>();
                }
            }

            return _instance;
        }
    }
}

public interface IOrderedDictionary<TKey, TValue> : IDictionary<TKey, TValue>
{
    IEnumerable<KeyValuePair<TKey, TValue>> GetReverseEnumerator();
    IEnumerable<KeyValuePair<TKey, TValue>> GetIncreasingEnumerator(TKey start);
    IEnumerable<KeyValuePair<TKey, TValue>> GetDecreasingEnumerator(TKey start);
    long RemoveRange(TKey start, bool includeStart, TKey end, bool includeEnd);
    IOrderedDictionaryEnumerator<TKey, TValue> GetAdvancedEnumerator(AdvancedEnumeratorParam<TKey> param);
}
