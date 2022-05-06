using System;
using BTDB.IL;

namespace BTDB.FieldHandler;

public interface IFieldHandlerWithInit
{
    bool NeedInit();
    void Init(IILGen ilGenerator, Action<IILGen>? pushReaderCtx);
}
