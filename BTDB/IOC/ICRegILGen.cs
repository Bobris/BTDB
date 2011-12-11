using System.Collections.Generic;
using BTDB.IL;

namespace BTDB.IOC
{
    internal interface ICRegILGen
    {
        string GenFuncName { get; }
        void GenInitialization(ContainerImpl container, IILGen il, IDictionary<string, object> context);
        IILLocal GenMain(ContainerImpl container, IILGen il, IDictionary<string, object> context);
    }
}