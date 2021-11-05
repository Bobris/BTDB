using System.Reflection.Emit;

namespace BTDB.IL;

internal interface IILFieldPrivate : IILField
{
    FieldBuilder TrueField { get; }
    void FreeTemps();
}
