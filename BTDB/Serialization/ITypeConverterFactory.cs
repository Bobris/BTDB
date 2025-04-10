using System;

namespace BTDB.Serialization;

public delegate void ActionConverter<TFrom, TTo>(in TFrom from, out TTo to);

public interface ITypeConverterFactory
{
    Converter? GetConverter(Type from, Type to);

    void RegisterConverter<TFrom, TTo>(ActionConverter<TFrom, TTo> converter);
}
