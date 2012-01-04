using System;
using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC
{
    internal class GenerationContext : IGenerationContext
    {
        readonly ContainerImpl _container;
        readonly Dictionary<Type, object> _specifics = new Dictionary<Type, object>();

        public GenerationContext(ContainerImpl container)
        {
            _container = container;
        }

        public IILGen IL { get; internal set; }

        public ContainerImpl Container
        {
            get { return _container; }
        }

        public T GetSpecific<T>() where T : class, new()
        {
            object specific;
            if (!_specifics.TryGetValue(typeof(T), out specific))
            {
                specific = new T();
                var contextSetter = specific as IGenerationContextSetter;
                if (contextSetter != null)
                    contextSetter.Set(this);
                _specifics.Add(typeof(T), specific);
            }
            return (T)specific;
        }
    }
}