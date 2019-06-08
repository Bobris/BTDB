using System;
using System.Text;

namespace BTDB.EventStoreLayer
{
    public class FullNameTypeMapper : ITypeNameMapper
    {
        public string ToName(Type type)
        {
            if (!type.IsGenericType)
                return type.FullName;

            var sb = new StringBuilder();
            ToName(type, sb);
            return sb.ToString();
        }

        void ToName(Type type, StringBuilder sb)
        {
            if (type.IsGenericType)
            {
                sb
                    .Append(type.Namespace)
                    .Append('.')
                    .Append(type.Name, 0, type.Name.IndexOf('`'))
                    .Append('<');

                var args = type.GetGenericArguments();
                for (int i = 0; i < args.Length; i++)
                {
                    if (i != 0)
                        sb.Append(',');
                    ToName(args[i], sb);
                }

                sb.Append('>');
            }
            else
            {
                sb.Append(type.FullName);
            }

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