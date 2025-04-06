using System;

namespace BTDB.Serialization;

public interface ITypeConverterFactory
{
    Converter? GetConverter(Type from, Type to);
}
