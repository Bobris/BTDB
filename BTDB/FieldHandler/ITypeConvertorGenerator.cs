using System;
using BTDB.IL;

namespace BTDB.FieldHandler;

public interface ITypeConvertorGenerator
{
    Action<IILGen>? GenerateConversion(Type from, Type to);
}
