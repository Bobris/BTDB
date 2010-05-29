using System;

namespace BTDB
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