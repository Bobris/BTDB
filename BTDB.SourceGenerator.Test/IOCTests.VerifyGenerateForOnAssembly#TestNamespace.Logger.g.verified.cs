﻿//HintName: TestNamespace.Logger.g.cs
// <auto-generated/>
using System;
using System.Runtime.CompilerServices;

namespace TestNamespace;

static file class LoggerRegistration
{
    [ModuleInitializer]
    internal static unsafe void Register4BTDB()
    {
        global::BTDB.IOC.IContainer.RegisterFactory(typeof(global::TestNamespace.Logger), (container, ctx) =>
        {
            return (container2, ctx2) =>
            {
                var res = new global::TestNamespace.Logger();
                return res;
            };
        });
    }
}
