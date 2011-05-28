using System;

namespace BTDB.KVDBLayer.Interface
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