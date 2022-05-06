using System;

namespace BTDB.IL;

public interface IILField
{
    Type FieldType { get; }
    string Name { get; }
}
