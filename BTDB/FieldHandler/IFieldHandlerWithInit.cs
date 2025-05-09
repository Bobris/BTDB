using System;
using BTDB.IL;
using BTDB.ODBLayer;

namespace BTDB.FieldHandler;

public delegate void FieldHandlerInit(IReaderCtx? ctx, ref byte value);

public interface IFieldHandlerWithInit
{
    bool NeedInit();
    void Init(IILGen ilGenerator, Action<IILGen>? pushReaderCtx);
    FieldHandlerInit Init();
}
