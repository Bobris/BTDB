using System;
using System.Reflection.Emit;

namespace BTDB.IL;

internal class ILFieldImpl : IILFieldPrivate
{
    public ILFieldImpl(FieldBuilder fieldBuilder)
    {
        TrueField = fieldBuilder;
    }

    public FieldBuilder TrueField { get; }

    public void FreeTemps()
    {
    }

    public Type FieldType => TrueField.FieldType;
    public string Name => TrueField.Name;
}
