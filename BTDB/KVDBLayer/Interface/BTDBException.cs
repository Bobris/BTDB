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
}
