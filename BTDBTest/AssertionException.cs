using System;
using System.Runtime.Serialization;

namespace BTDBTest;

[Serializable]
internal class AssertionException : Exception
{
    public AssertionException()
    {
    }

    public AssertionException(string message) : base(message)
    {
    }

    public AssertionException(string message, Exception innerException) : base(message, innerException)
    {
    }

    #pragma warning disable SYSLIB0051
    protected AssertionException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }
    #pragma warning restore SYSLIB0051
}
