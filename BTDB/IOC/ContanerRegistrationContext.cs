using System;
using System.Collections.Generic;

namespace BTDB.IOC
{
    internal class ContanerRegistrationContext
    {
        readonly ContainerImpl _container;
        readonly Dictionary<KeyValuePair<object, Type>, ICReg> _registrations;
        readonly List<object> _instances = new List<object>();

        internal ContanerRegistrationContext(ContainerImpl container, Dictionary<KeyValuePair<object, Type>, ICReg> registrations)
        {
            _container = container;
            _registrations = registrations;
        }

        internal int SingletonCount { get; set; }

        internal List<object> Instances
        {
            get { return _instances; }
        }

        internal int AddInstance(object instance)
        {
            _instances.Add(instance);
            return _instances.Count - 1;
        }

        public void AddCReg(IEnumerable<KeyValuePair<object, Type>> asTypes, ICReg registration)
        {
            foreach (var asType in asTypes)
            {
                AddCReg(asType, registration);
            }
        }

        void AddCReg(KeyValuePair<object, Type> asType, ICReg registration)
        {
            _registrations.Add(asType, registration);
        }
    }
}