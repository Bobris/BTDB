using BTDB.IL;

namespace BTDB.IOC.CRegs
{
    internal class InstancesLocalGenCtxHelper : IGenerationContextSetter
    {
        IGenerationContext _context;

        public void Set(IGenerationContext context)
        {
            _context = context;
        }

        internal void Prepare(ICRegILGen parentReg)
        {
            if (MainLocal != null) return;
            MainLocal = _context.IL.DeclareLocal(typeof(object[]), "instances");
            _context.PushToILStack(parentReg, Need.ContainerNeed);
            _context.IL
                .Ldfld(() => default(ContainerImpl).Instances)
                .Stloc(MainLocal);
        }

        internal IILLocal MainLocal { get; private set; }
    }
}