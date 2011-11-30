using System;
using System.Runtime.Serialization;

namespace BTDB.KVDBLayer
{
    [Serializable]
    public class BTDBException : Exception
    {
        public BTDBException(string message)
            : base(message)
        {
        }
 
        BTDBException(SerializationInfo info, StreamingContext context) 
            : base(info, context)
        {
        }
    }
}