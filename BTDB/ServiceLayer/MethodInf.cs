using System.Reflection;

namespace BTDB.ServiceLayer
{
    public class MethodInf
    {
        readonly string _name;
        readonly string _ifaceName;
        readonly ParameterInf[] _parameters;

        public MethodInf(MethodInfo method)
        {
            _name = method.Name;
            var methodBase = method.GetBaseDefinition();
            if (methodBase != method) _ifaceName = methodBase.DeclaringType.Name;
            var parameterInfos = method.GetParameters();
            _parameters = new ParameterInf[parameterInfos.Length];
            for (int i = 0; i < parameterInfos.Length; i++)
            {
                _parameters[i] = new ParameterInf(parameterInfos[i]);
            }
        }

        public string Name
        {
            get { return _name; }
        }

        public string IfaceName
        {
            get { return _ifaceName; }
        }
    }
}