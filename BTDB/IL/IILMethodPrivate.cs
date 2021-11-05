using System;
using System.Reflection;

namespace BTDB.IL;

internal interface IILMethodPrivate : IILMethod
{
    MethodInfo TrueMethodInfo { get; }
    Type ReturnType { get; }
    Type[] Parameters { get; }
}
