using System.Collections.Generic;

namespace BTDB.IOC
{
    internal interface ICRegMulti : ICReg
    {
        ICReg ChosenOne { get; }
        IEnumerable<ICReg> Regs { get; }
        void Add(ICReg reg, bool chosenOne);
    }

    internal class CRegMulti : ICRegMulti
    {
        ICReg _chosenOne;
        readonly IList<ICReg> _regs = new List<ICReg>();


        public ICReg ChosenOne
        {
            get { return _chosenOne; }
        }

        public IEnumerable<ICReg> Regs
        {
            get { return _regs; }
        }

        public void Add(ICReg reg, bool preserveExistingDefaults)
        {
            if (!preserveExistingDefaults || _regs.Count == 0) _chosenOne = reg;
            _regs.Add(reg);
        }
    }
}