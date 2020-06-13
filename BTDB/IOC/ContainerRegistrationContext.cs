using System.Collections.Generic;
using BTDB.Collections;

namespace BTDB.IOC
{
    class ContainerRegistrationContext
    {
        readonly ContainerImpl _container;
        readonly Dictionary<KeyAndType, ICReg> _registrations;
        StructList<object> _instances;

        internal ContainerRegistrationContext(ContainerImpl container, Dictionary<KeyAndType, ICReg> registrations)
        {
            _container = container;
            _registrations = registrations;
        }

        internal int SingletonCount { get; set; }

        internal object[] Instances => _instances.ToArray();

        internal int AddInstance(object instance)
        {
            _instances.Add(instance);
            return (int)_instances.Count - 1;
        }

        public void AddCReg(IEnumerable<KeyAndType> asTypes, bool preserveExistingDefaults, ICReg registration)
        {
            foreach (var asType in asTypes)
            {
                AddCReg(asType, preserveExistingDefaults, registration);
            }
        }

        void AddCReg(KeyAndType asType, bool preserveExistingDefaults, ICReg registration)
        {
            ICReg currentReg;
            if (!_registrations.TryGetValue(asType, out currentReg))
            {
                _registrations.Add(asType, registration);
                return;
            }
            var multi = currentReg as ICRegMulti;
            if (multi != null)
            {
                multi.Add(registration, preserveExistingDefaults);
                return;
            }
            multi = new CRegMulti();
            multi.Add(currentReg, false);
            multi.Add(registration, preserveExistingDefaults);
            _registrations[asType] = multi;
        }
    }
}
