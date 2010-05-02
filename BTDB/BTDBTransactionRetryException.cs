using System;

namespace BTDB
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