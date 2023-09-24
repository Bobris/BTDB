using System;
using System.Runtime.Serialization;

namespace BTDB.KVDBLayer;

[Serializable]
public class BTDBTransactionRetryException : Exception
{
    public BTDBTransactionRetryException(string message)
        : base(message)
    {
    }
}
