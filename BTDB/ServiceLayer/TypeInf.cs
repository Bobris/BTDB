using System;
using System.Collections.Generic;
using System.Reflection;

namespace BTDB.ServiceLayer
{
    public class TypeInf
    {
        readonly string _name;
        readonly MethodInf[] _methodInfs;

        public TypeInf(Type type)
        {
            _name = type.Name;
            var methodInfs = new List<MethodInf>();
            var methods = type.GetMethods();
            foreach (var method in methods)
            {
                var methodBase = method.GetBaseDefinition();
                if (methodBase.DeclaringType == typeof(object)) continue;
                if (!methodBase.IsPublic) continue;
                if (!IsMethodSupported(method)) continue;
                methodInfs.Add(new MethodInf(method));
            }
            _methodInfs = methodInfs.ToArray();
        }

        public string Name
        {
            get { return _name; }
        }

        public MethodInf[] MethodInfs
        {
            get { return _methodInfs; }
        }

        static bool IsMethodSupported(MethodInfo method)
        {
            if (!IsSupportedType(method.ReturnType)) return false;
            foreach (var parameter in method.GetParameters())
            {
                if (parameter.IsOptional) return false;
                if (parameter.IsOut) return false;
                if (parameter.IsRetval) return false;
                if (!IsSupportedType(parameter.ParameterType)) return false;
            }
            return true;
        }

        static bool IsSupportedType(Type type)
        {
            if (type == typeof(int)) return true;
            return false;
        }
    
    }
}