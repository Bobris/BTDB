using System;
using System.Collections.Generic;
using System.Reflection.Emit;
using BTDB.IL;

namespace BTDB.ODBLayer
{
    public class DefaultTypeConvertorGenerator : ITypeConvertorGenerator
    {
        readonly Dictionary<Tuple<Type, Type>, Action<ILGenerator>> _conversions = new Dictionary<Tuple<Type, Type>, Action<ILGenerator>>();

        public DefaultTypeConvertorGenerator()
        {
            var convConvertibleTypes = new[]
                                           {
                                               typeof (byte), typeof (sbyte), typeof (ushort), typeof (short),
                                               typeof (uint), typeof (int), typeof (ulong), typeof (long)
                                           };
            AddConversions(convConvertibleTypes, typeof(long), ilg => ilg.ConvI8());
            AddConversions(convConvertibleTypes, typeof(ulong), ilg => ilg.ConvU8());
            AddConversions(convConvertibleTypes, typeof(int), ilg => ilg.ConvI4());
            AddConversions(convConvertibleTypes, typeof(uint), ilg => ilg.ConvU4());
            AddConversions(convConvertibleTypes, typeof(short), ilg => ilg.ConvI2());
            AddConversions(convConvertibleTypes, typeof(ushort), ilg => ilg.ConvU2());
            AddConversions(convConvertibleTypes, typeof(sbyte), ilg => ilg.ConvI1());
            AddConversions(convConvertibleTypes, typeof(byte), ilg => ilg.ConvU1());
        }

        void AddConversions(IEnumerable<Type> fromList, Type to, Action<ILGenerator> generator)
        {
            foreach (var from in fromList)
            {
                _conversions[Tuple.Create(from, to)] = generator;
            }
        }

        public Action<ILGenerator> GenerateConversion(Type from, Type to)
        {
            if (from == to) return ilg => { };
            Action<ILGenerator> generator;
            if (_conversions.TryGetValue(new Tuple<Type, Type>(from, to), out generator))
            {
                return generator;
            }
            return null;
        }

        public Type CanConvertThrough(Type from, Func<Type, bool> toFilter)
        {
            if (toFilter(from)) return from;
            foreach (var conversion in _conversions)
            {
                if (conversion.Key.Item1 != from) continue;
                if (toFilter(conversion.Key.Item2)) return conversion.Key.Item2;
            }
            return null;
        }
    }
}