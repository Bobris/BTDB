using System;

namespace BTDB.IL
{
    public interface IILDynamicType
    {
        IILMethod NewMethod(string name, Type returns, Type[] parameters);
    }
}