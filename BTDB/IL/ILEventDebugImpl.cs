using System.Reflection.Emit;

namespace BTDB.IL
{
    internal class ILEventDebugImpl : IILEvent
    {
        readonly EventBuilder _eventBuilder;

        public ILEventDebugImpl(EventBuilder eventBuilder)
        {
            _eventBuilder = eventBuilder;
        }

        public void SetAddOnMethod(IILMethod method)
        {
            _eventBuilder.SetAddOnMethod(((ILMethodDebugImpl)method).MethodBuilder);
        }

        public void SetRemoveOnMethod(IILMethod method)
        {
            _eventBuilder.SetRemoveOnMethod(((ILMethodDebugImpl)method).MethodBuilder);
        }
    }
}