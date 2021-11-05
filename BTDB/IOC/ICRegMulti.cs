using System.Collections.Generic;

namespace BTDB.IOC;

interface ICRegMulti : ICReg
{
    ICReg ChosenOne { get; }
    IEnumerable<ICReg> Regs { get; }
    void Add(ICReg reg, bool chosenOne);
}

class CRegMulti : ICRegMulti
{
    ICReg _chosenOne;
    readonly IList<ICReg> _regs = new List<ICReg>();


    public ICReg ChosenOne => _chosenOne;

    public IEnumerable<ICReg> Regs => _regs;

    public void Add(ICReg reg, bool preserveExistingDefaults)
    {
        if (!preserveExistingDefaults || _regs.Count == 0) _chosenOne = reg;
        _regs.Add(reg);
    }

    public void Verify(ContainerVerification options, ContainerImpl container)
    {
        foreach (var reg in _regs)
        {
            reg.Verify(options, container);
        }
    }
}
