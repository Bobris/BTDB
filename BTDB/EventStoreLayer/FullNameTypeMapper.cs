using System;

namespace BTDB.EventStoreLayer
{
    public class FullNameTypeMapper : ITypeNameMapper
    {
        public string ToName(Type type)
        {
            return type.FullName;
        }

        public Type ToType(string name)
        {
            var t = Type.GetType(name);
            if (t != null)
                return t;
            foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
            {
                t = asm.GetType(name);
                if (t != null)
                    return t;
            }
            return null;
        }
    }
}