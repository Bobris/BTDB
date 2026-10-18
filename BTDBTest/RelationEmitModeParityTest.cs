using System;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// The classes below rerun existing relation test suites with IFieldHandler.UseNoEmitForRelations = false, so every
// relation method shape is built by the IL emitting RelationBuilder instead of the source generated creators.
// They live in the non-parallel "IFieldHandler.UseNoEmitForRelations" collection because the switch is global.

static class RelationEmitMode
{
    internal static bool Enter()
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        IFieldHandler.UseNoEmitForRelations = false;
        ObjectDB.ResetAllMetadataCaches();
        return old;
    }

    internal static void Exit(bool old)
    {
        IFieldHandler.UseNoEmitForRelations = old;
        ObjectDB.ResetAllMetadataCaches();
    }

    internal static T EnterAndPass<T>(T value, out bool old)
    {
        old = Enter();
        return value;
    }
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbTableEmitModeTest : ObjectDbTableTest, IDisposable
{
    readonly bool _old;

    public ObjectDbTableEmitModeTest(ITestOutputHelper output) : base(RelationEmitMode.EnterAndPass(output, out var old))
    {
        _old = old;
    }

    void IDisposable.Dispose()
    {
        try
        {
            Dispose();
        }
        finally
        {
            RelationEmitMode.Exit(_old);
        }
    }
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbTableScanEmitModeTest : ObjectDbTableScanTest, IDisposable
{
    readonly bool _old;

    public ObjectDbTableScanEmitModeTest(ITestOutputHelper output) : base(
        RelationEmitMode.EnterAndPass(output, out var old))
    {
        _old = old;
    }

    void IDisposable.Dispose()
    {
        try
        {
            Dispose();
        }
        finally
        {
            RelationEmitMode.Exit(_old);
        }
    }
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbTableTupleEmitModeTest : ObjectDbTableTupleTest, IDisposable
{
    readonly bool _old;

    public ObjectDbTableTupleEmitModeTest(ITestOutputHelper output) : base(
        RelationEmitMode.EnterAndPass(output, out var old))
    {
        _old = old;
    }

    void IDisposable.Dispose()
    {
        try
        {
            Dispose();
        }
        finally
        {
            RelationEmitMode.Exit(_old);
        }
    }
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbTableInKeyValueEmitModeTest : ObjectDbTableInKeyValueTest, IDisposable
{
    readonly bool _old;

    public ObjectDbTableInKeyValueEmitModeTest(ITestOutputHelper output) : base(
        RelationEmitMode.EnterAndPass(output, out var old))
    {
        _old = old;
    }

    void IDisposable.Dispose()
    {
        try
        {
            Dispose();
        }
        finally
        {
            RelationEmitMode.Exit(_old);
        }
    }
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbTableUpdateByIdEmitModeTest : ObjectDbTableUpdateByIdTest, IDisposable
{
    readonly bool _old;

    public ObjectDbTableUpdateByIdEmitModeTest(ITestOutputHelper output) : base(
        RelationEmitMode.EnterAndPass(output, out var old))
    {
        _old = old;
    }

    void IDisposable.Dispose()
    {
        try
        {
            Dispose();
        }
        finally
        {
            RelationEmitMode.Exit(_old);
        }
    }
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbTableUpdateByIdAllTypesEmitModeTest : ObjectDbTableUpdateByIdAllTypesTest, IDisposable
{
    readonly bool _old;

    public ObjectDbTableUpdateByIdAllTypesEmitModeTest(ITestOutputHelper output) : base(
        RelationEmitMode.EnterAndPass(output, out var old))
    {
        _old = old;
    }

    void IDisposable.Dispose()
    {
        try
        {
            Dispose();
        }
        finally
        {
            RelationEmitMode.Exit(_old);
        }
    }
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbTableWithSizesEmitModeTest : ObjectDbTableWithSizesTest, IDisposable
{
    readonly bool _old;

    public ObjectDbTableWithSizesEmitModeTest(ITestOutputHelper output) : base(
        RelationEmitMode.EnterAndPass(output, out var old))
    {
        _old = old;
    }

    void IDisposable.Dispose()
    {
        try
        {
            Dispose();
        }
        finally
        {
            RelationEmitMode.Exit(_old);
        }
    }
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbTableFreeContentEmitModeTest : ObjectDbTableFreeContentTest, IDisposable
{
    readonly bool _old;

    public ObjectDbTableFreeContentEmitModeTest() : base()
    {
        _old = RelationEmitMode.Enter();
    }

    void IDisposable.Dispose()
    {
        try
        {
            Dispose();
        }
        finally
        {
            RelationEmitMode.Exit(_old);
        }
    }
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class InKeyValueSecondaryKeyAdvancedEnumeratorEmitModeTest : InKeyValueSecondaryKeyAdvancedEnumeratorTest,
    IDisposable
{
    readonly bool _old;

    public InKeyValueSecondaryKeyAdvancedEnumeratorEmitModeTest(ITestOutputHelper output) : base(
        RelationEmitMode.EnterAndPass(output, out var old))
    {
        _old = old;
    }

    void IDisposable.Dispose()
    {
        try
        {
            Dispose();
        }
        finally
        {
            RelationEmitMode.Exit(_old);
        }
    }
}

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbAdvancedEnumeratorBoundaryEmitModeTest : ObjectDbAdvancedEnumeratorBoundaryTest, IDisposable
{
    readonly bool _old;

    public ObjectDbAdvancedEnumeratorBoundaryEmitModeTest(ITestOutputHelper output) : base(
        RelationEmitMode.EnterAndPass(output, out var old))
    {
        _old = old;
    }

    void IDisposable.Dispose()
    {
        try
        {
            Dispose();
        }
        finally
        {
            RelationEmitMode.Exit(_old);
        }
    }
}
