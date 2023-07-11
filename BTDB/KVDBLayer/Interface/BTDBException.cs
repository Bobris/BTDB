using System;
using System.Runtime.Serialization;
using System.Text;

namespace BTDB.KVDBLayer;

[Serializable]
public class BTDBException : Exception
{
    public BTDBException(string message)
        : base(message)
    {
    }

    public BTDBException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    BTDBException(SerializationInfo info, StreamingContext context)
        : base(info, context)
    {
    }

    public static void ThrowNonUniqueKey(ReadOnlySpan<byte> keyPrefix)
    {
        var sb = new StringBuilder();
        foreach (var b in keyPrefix)
        {
            sb.Append($"{b:X2}");
        }
        throw new BTDBException("KeyPrefix in UpdateKeySuffix is not unique: "+sb);
    }
}
