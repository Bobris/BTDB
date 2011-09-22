using System;
using System.Reflection.Emit;

namespace BTDB.FieldHandler
{
    public interface ITypeConvertorGenerator
    {
        Action<ILGenerator> GenerateConversion(Type from, Type to);
    }
}
