using System;
using System.Collections.Concurrent;

namespace BTDB.ODBLayer
{
    internal class Type2NameRegistry
    {
        public Type2NameRegistry()
        {
            _name2Type = new ConcurrentDictionary<string, Type>(ReferenceEqualityComparer<string>.Instance);
            _type2Name = new ConcurrentDictionary<Type, string>(ReferenceEqualityComparer<Type>.Instance);
        }

        readonly ConcurrentDictionary<string, Type> _name2Type;
        readonly ConcurrentDictionary<Type, string> _type2Name;

        internal string RegisterType(Type type, string asName)
        {
            asName = string.Intern(asName);
            if (FindNameByType(type) == asName && FindTypeByName(asName) == type) return asName;
            _name2Type.AddOrUpdate(asName, type, (a, b) => type);
            _type2Name.AddOrUpdate(type, asName, (a, b) => asName);
            return asName;
        }

        internal Type FindTypeByName(string name)
        {
            Type result;
            if (_name2Type.TryGetValue(name, out result)) return result;
            return null;
        }

        internal string FindNameByType(Type type)
        {
            string result;
            if (_type2Name.TryGetValue(type, out result)) return result;
            return null;
        }
    }
}