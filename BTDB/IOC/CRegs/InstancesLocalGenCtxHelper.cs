using BTDB.IL;

namespace BTDB.IOC.CRegs;

class InstancesLocalGenCtxHelper : IGenerationContextSetter
{
    IGenerationContext _context;

    public void Set(IGenerationContext context)
    {
        _context = context;
    }

    internal void Prepare()
    {
        if (MainLocal != null) return;
        MainLocal = _context.IL.DeclareLocal(typeof(object[]), "instances");
        _context.PushToILStack(Need.ContainerNeed);
        _context.IL
            .Castclass(typeof(ContainerImpl))
            .Ldfld(() => default(ContainerImpl).Instances)
            .Stloc(MainLocal);
    }

    internal IILLocal MainLocal { get; private set; }
}
