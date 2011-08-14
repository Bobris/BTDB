using System.Reflection;
using BTDB.KVDBLayer.ReaderWriters;

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

        public MethodInf(AbstractBufferedReader reader)
        {
            _name = reader.ReadString();
            _ifaceName = reader.ReadString();
            var parameterCount = reader.ReadVUInt32();
            _parameters = new ParameterInf[parameterCount];
            for (int i = 0; i < _parameters.Length; i++)
            {
                _parameters[i]=new ParameterInf(reader);
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

        public void Store(AbstractBufferedWriter writer)
        {
            writer.WriteString(_name);
            writer.WriteString(_ifaceName);
            writer.WriteVUInt32((uint) _parameters.Length);
            foreach (var parameter in _parameters)
            {
                parameter.Store(writer);
            }
        }
    }
}