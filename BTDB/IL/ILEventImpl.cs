using System.Reflection.Emit;

namespace BTDB.IL;

class ILEventImpl : IILEvent
{
    readonly EventBuilder _eventBuilder;

    public ILEventImpl(EventBuilder eventBuilder)
    {
        _eventBuilder = eventBuilder;
    }

    public void SetAddOnMethod(IILMethod method)
    {
        _eventBuilder.SetAddOnMethod(((ILMethodImpl)method).MethodBuilder);
    }

    public void SetRemoveOnMethod(IILMethod method)
    {
        _eventBuilder.SetRemoveOnMethod(((ILMethodImpl)method).MethodBuilder);
    }
}
