using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BTDB
{
    static class ExtensionMethods
    {
        public static bool CreateKey(this ILowLevelDBTransaction transaction, byte[] keyBuf)
        {
            return transaction.FindKey(keyBuf, 0, keyBuf.Length, FindKeyStrategy.Create)==FindKeyResult.Created;
        }

        public static bool FindExactKey(this ILowLevelDBTransaction transaction, byte[] keyBuf)
        {
            return transaction.FindKey(keyBuf, 0, keyBuf.Length, FindKeyStrategy.ExactMatch)==FindKeyResult.FoundExact;
        }
    }
}
