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

        public static byte[] ReadKey(this ILowLevelDBTransaction transaction)
        {
            int keySize = transaction.GetKeySize();
            if (keySize < 0) return null;
            var result = new byte[keySize];
            transaction.ReadKey(0, keySize, result, 0);
            return result;
        }

        public static byte[] ReadValue(this ILowLevelDBTransaction transaction)
        {
            long valueSize = transaction.GetValueSize();
            if (valueSize < 0) return null;
            if ((int)valueSize!=valueSize) throw new BTDBException("Value is bigger then 2GB does not fit in byte[]");
            var result = new byte[valueSize];
            transaction.ReadValue(0, (int)valueSize, result, 0);
            return result;
        }
    }
}
