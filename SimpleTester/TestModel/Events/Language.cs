using System;

namespace SimpleTester.TestModel.Events;

public enum Language
{
    EnUs = 0,
    EnUk = 1,
    FrFr = 2,
    EsEs = 3,
    DeDe = 4,
    ItIt = 5
}

[Flags]
public enum Languages
{
    EnUs = 1 << 0,
    EnUk = 1 << 1,
    De = 1 << 2
}
