using BTDB.Buffer;

namespace BTDB.KV2DBLayer
{
    public static class ExtensionMethods
    {
        public static bool CreateKeyUnsafe(this IKeyValue2DBTransaction transaction, byte[] keyBuf)
        {
            if (FindExactKey(transaction, keyBuf)) return false;
            return transaction.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(keyBuf), ByteBuffer.NewEmpty());
        }

        public static bool CreateKey(this IKeyValue2DBTransaction transaction, byte[] keyBuf)
        {
            if (FindExactKey(transaction, keyBuf)) return false;
            return transaction.CreateOrUpdateKeyValue(ByteBuffer.NewSync(keyBuf), ByteBuffer.NewEmpty());
        }

        public static bool FindExactKey(this IKeyValue2DBTransaction transaction, byte[] keyBuf)
        {
            return transaction.Find(ByteBuffer.NewSync(keyBuf)) == FindResult.Exact;
        }

        public static bool CreateOrUpdateKeyValueUnsafe(this IKeyValue2DBTransaction transaction, byte[] keyBuf, byte[] valueBuf)
        {
            return transaction.CreateOrUpdateKeyValue(ByteBuffer.NewAsync(keyBuf), ByteBuffer.NewAsync(valueBuf));
        }

        public static bool CreateOrUpdateKeyValue(this IKeyValue2DBTransaction transaction, byte[] keyBuf, byte[] valueBuf)
        {
            return transaction.CreateOrUpdateKeyValue(ByteBuffer.NewSync(keyBuf), ByteBuffer.NewSync(valueBuf));
        }

        public static void SetValue(this IKeyValue2DBTransaction transaction, byte[] valueBuf)
        {
            transaction.SetValue(ByteBuffer.NewSync(valueBuf));
        }

        public static void SetKeyPrefixUnsafe(this IKeyValue2DBTransaction transaction, byte[] prefix)
        {
            transaction.SetKeyPrefix(prefix==null?ByteBuffer.NewEmpty():ByteBuffer.NewAsync(prefix));
        }

        public static void SetKeyPrefix(this IKeyValue2DBTransaction transaction, byte[] prefix)
        {
            transaction.SetKeyPrefix(prefix == null ? ByteBuffer.NewEmpty() : ByteBuffer.NewSync(prefix));
        }

        public static byte[] GetKeyAsByteArray(this IKeyValue2DBTransaction transaction)
        {
            return transaction.GetKey().ToByteArray();
        }

        public static byte[] GetValueAsByteArray(this IKeyValue2DBTransaction transaction)
        {
            return transaction.GetValue().ToByteArray();
        }
    }
}
