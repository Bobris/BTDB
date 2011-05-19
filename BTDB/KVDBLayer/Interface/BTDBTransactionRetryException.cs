using System;

namespace BTDB.KVDBLayer.Interface
{
    [Serializable]
    public class BTDBTransactionRetryException : Exception
    {
        public BTDBTransactionRetryException(string message)
            : base(message)
        {
        }
    }
}