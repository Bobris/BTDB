using System;
using System.Collections.Concurrent;

namespace BTDB.ODBLayer;

public static class ConcurrentDictionaryExtensions
{
    public static TValue GetOrAdd<TKey, TValue, TArg>(
            this ConcurrentDictionary<TKey, TValue> dictionary,
            TKey key,
            Func<TKey, TArg, TValue> valueFactory,
            TArg arg)
    {
        while (true)
        {
            TValue value;
            if (dictionary.TryGetValue(key, out value))
                return value;

            value = valueFactory(key, arg);
            if (dictionary.TryAdd(key, value))
                return value;
        }
    }

    public static TValue GetOrAdd<TKey, TValue, TArg1, TArg2>(
            this ConcurrentDictionary<TKey, TValue> dictionary,
            TKey key,
            Func<TKey, TArg1, TArg2, TValue> valueFactory,
            TArg1 arg1, TArg2 arg2)
    {
        while (true)
        {
            TValue value;
            if (dictionary.TryGetValue(key, out value))
                return value;

            value = valueFactory(key, arg1, arg2);
            if (dictionary.TryAdd(key, value))
                return value;
        }
    }

    public static TValue GetOrAdd<TKey, TValue, TArg1, TArg2, TArg3>(
            this ConcurrentDictionary<TKey, TValue> dictionary,
            TKey key,
            Func<TKey, TArg1, TArg2, TArg3, TValue> valueFactory,
            TArg1 arg1, TArg2 arg2, TArg3 arg3)
    {
        while (true)
        {
            TValue value;
            if (dictionary.TryGetValue(key, out value))
                return value;

            value = valueFactory(key, arg1, arg2, arg3);
            if (dictionary.TryAdd(key, value))
                return value;
        }
    }
}
