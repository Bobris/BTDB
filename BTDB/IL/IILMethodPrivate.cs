using System.Reflection;

namespace BTDB.IL
{
    internal interface IILMethodPrivate : IILMethod
    {
        MethodInfo TrueMethodInfo { get; }
    }
}