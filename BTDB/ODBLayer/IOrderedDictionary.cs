using System.Collections.Generic;

namespace BTDB.ODBLayer
{
    public interface IOrderedDictionaryEnumerator<TKey, TValue>
    {
        /// <summary>
        /// Read current cursor position key and move cursor to next pair, if at end just return false and stay at end
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

    public class AdvancedEnumeratorParam<TKey>
    {
        public AdvancedEnumeratorParam()
        {
            order = EnumerationOrder.Ascending;
            startProposition = KeyProposition.Ignored;
            endProposition = KeyProposition.Ignored;
        }

        public AdvancedEnumeratorParam(EnumerationOrder order)
        {
            this.order = order;
            startProposition = KeyProposition.Ignored;
            endProposition = KeyProposition.Ignored;
        }

        public AdvancedEnumeratorParam(EnumerationOrder order, TKey start, bool included = true)
        {
            this.order = order;
            this.start = start;
            startProposition = included ? KeyProposition.Included : KeyProposition.Excluded;
            endProposition = KeyProposition.Ignored;
        }

        public AdvancedEnumeratorParam(EnumerationOrder order, TKey start, TKey end, bool endIncluded = false)
        {
            this.order = order;
            this.start = start;
            startProposition = KeyProposition.Included;
            this.end = end;
            endProposition = endIncluded ? KeyProposition.Included : KeyProposition.Excluded;
        }

        public AdvancedEnumeratorParam(EnumerationOrder order, TKey start, KeyProposition startProposition, TKey end, KeyProposition endProposition)
        {
            this.order = order;
            this.start = start;
            this.startProposition = startProposition;
            this.end = end;
            this.endProposition = endProposition;
        }

        public EnumerationOrder order;
        public KeyProposition startProposition;
        public TKey start;
        public KeyProposition endProposition;
        public TKey end;
    }

    public interface IOrderedDictionary<TKey, TValue> : IDictionary<TKey,TValue>
    {
        IEnumerable<KeyValuePair<TKey, TValue>> GetReverseEnumerator();
        IEnumerable<KeyValuePair<TKey, TValue>> GetIncreasingEnumerator(TKey start);
        IEnumerable<KeyValuePair<TKey, TValue>> GetDecreasingEnumerator(TKey start);
        long RemoveRange(TKey start, bool includeStart, TKey end, bool includeEnd);
        IOrderedDictionaryEnumerator<TKey, TValue> GetAdvancedEnumerator(AdvancedEnumeratorParam<TKey> param);
    }
}