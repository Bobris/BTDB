using System;

namespace BTDB.KVDBLayer
{
    [Serializable]
    public class BTDBException : Exception
    {
        public BTDBException(string message)
            : base(message)
        {
        }
    }
}