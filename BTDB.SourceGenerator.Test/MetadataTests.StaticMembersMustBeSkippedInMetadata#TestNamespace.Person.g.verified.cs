﻿//HintName: TestNamespace.Person.g.cs
// <auto-generated/>
#pragma warning disable 612,618
using System;
using System.Runtime.CompilerServices;

namespace TestNamespace;

[CompilerGenerated]
static file class PersonRegistration
{
    [ModuleInitializer]
    internal static unsafe void Register4BTDB()
    {
        global::BTDB.IOC.IContainer.RegisterFactory(typeof(global::TestNamespace.Person), (container, ctx) =>
        {
            return (container2, ctx2) =>
            {
                var res = new global::TestNamespace.Person();
                return res;
            };
        });
    }
}
