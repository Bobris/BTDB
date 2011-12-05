using System;
using BTDB.IL;

namespace BTDB.FieldHandler
{
    public interface IFieldHandlerWithInit
    {
        void Init(IILGen ilGenerator, Action<IILGen> pushReaderCtx);
    }
}