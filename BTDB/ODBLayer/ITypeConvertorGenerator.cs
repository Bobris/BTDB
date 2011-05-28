using System;
using System.Reflection.Emit;

namespace BTDB.ODBLayer
{
    public interface ITypeConvertorGenerator
    {
        Action<ILGenerator> GenerateConversion(Type from, Type to);
        Type CanConvertThrough(Type from, Func<Type, bool> toFilter);
    }
}
