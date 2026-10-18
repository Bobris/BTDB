using System;
using System.Collections.Generic;
using System.IO;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class DefaultFieldHandlerLoggerTest : IDisposable
{
    readonly IKeyValueDB _kv = new InMemoryKeyValueDB();
    readonly ObjectDB _db = new();
    readonly List<string> _messages = new();
    readonly DefaultFieldHandlerLogger _logger;

    public DefaultFieldHandlerLoggerTest()
    {
        _db.Open(_kv, false);
        _logger = new DefaultFieldHandlerLogger(_messages.Add);
    }

    public void Dispose()
    {
        _db.Dispose();
        _kv.Dispose();
    }

    public enum E1
    {
        A,
        B
    }

    public enum E2
    {
        A,
        C
    }

    IFieldHandler H(Type type, FieldHandlerOptions options = FieldHandlerOptions.None) =>
        _db.FieldHandlerFactory.CreateFromType(type, options)!;

    string Report(Type? sourceType, IFieldHandler source, Type targetType, IFieldHandler? target)
    {
        _messages.Clear();
        _logger.ReportTypeIncompatibility(sourceType, source, targetType, target);
        return Assert.Single(_messages);
    }

    [Fact]
    public void NestedItemDifferenceIsDescribed()
    {
        var msg = Report(null, H(typeof(List<int>)), typeof(List<string>), H(typeof(List<string>)));
        Assert.Equal("Cannot load BTDB.FieldHandler.ListFieldHandler into BTDB.FieldHandler.ListFieldHandler " +
                     "(target: System.Collections.Generic.List<string>; item: BTDB.FieldHandler.SignedFieldHandler<long> -> " +
                     "BTDB.FieldHandler.SimpleFieldHandlerBase<string>)", msg);
    }

    [Fact]
    public void DeeplyNestedDifferenceUsesPath()
    {
        var msg = Report(null, H(typeof(Dictionary<int, List<int>>)), typeof(Dictionary<int, List<E1>>),
            H(typeof(Dictionary<int, List<E1>>)));
        Assert.EndsWith("value.item: BTDB.FieldHandler.SignedFieldHandler<long> -> Enum BTDBTest.DefaultFieldHandlerLoggerTest.E1{A=0,B=1})", msg);
        Assert.DoesNotContain("key:", msg);
    }

    [Fact]
    public void EnumDifferencesAreDescribedByMembers()
    {
        var msg = Report(null, H(typeof(List<E1>)), typeof(List<E2>), H(typeof(List<E2>)));
        Assert.EndsWith("item: Enum BTDBTest.DefaultFieldHandlerLoggerTest.E1{A=0,B=1} -> " +
                        "Enum BTDBTest.DefaultFieldHandlerLoggerTest.E2{A=0,C=1})", msg);
    }

    [Fact]
    public void NestedHandlerCountDifferenceIsReported()
    {
        var msg = Report(null, H(typeof((int, int))), typeof((int, int, int)), H(typeof((int, int, int))));
        Assert.Contains("nested handler count: 2 -> 3", msg);
    }

    [Fact]
    public void NestedHandlersWithoutDifferenceFallBackToTypes()
    {
        var msg = Report(null, H(typeof(List<int>)), typeof(int), H(typeof(List<int>)));
        Assert.EndsWith("(target: int; System.Collections.Generic.List<int> -> int)", msg);
        var withSourceType = Report(typeof(string), H(typeof(List<int>)), typeof(int), H(typeof(List<int>)));
        Assert.EndsWith("(target: int; string -> int)", withSourceType);
    }

    [Fact]
    public void SimpleHandlersDescribeTypes()
    {
        var msg = Report(null, H(typeof(string)), typeof(int), H(typeof(int)));
        Assert.Equal("Cannot load BTDB.FieldHandler.SimpleFieldHandlerBase into BTDB.FieldHandler.SignedFieldHandler " +
                     "(target: int; string -> int)", msg);
    }

    [Fact]
    public void MissingTargetHandlerIsReported()
    {
        Assert.Equal("Cannot load string into int", Report(typeof(string), H(typeof(string)), typeof(int), null));
        var msg = Report(null, H(typeof(string)), typeof(int), null);
        Assert.Equal("Cannot load BTDB.FieldHandler.SimpleFieldHandlerBase into int (string -> int)", msg);
        var withThrowingHandler = Report(null, new ThrowingHandler(), typeof(int), null);
        Assert.Equal("Cannot load " + typeof(ThrowingHandler).FullName + " into int", withThrowingHandler);
        var withThrowingTarget = Report(null, new ThrowingHandler(), typeof(int), new ThrowingHandler());
        Assert.EndsWith("(target: int)", withThrowingTarget);
    }

    [Fact]
    public void DefaultLoggerWritesToConsole()
    {
        var oldOut = Console.Out;
        var sw = new StringWriter();
        Console.SetOut(sw);
        try
        {
            new DefaultFieldHandlerLogger().ReportTypeIncompatibility(typeof(string), H(typeof(string)), typeof(int),
                null);
        }
        finally
        {
            Console.SetOut(oldOut);
        }

        Assert.Contains("Cannot load string into int", sw.ToString());
    }

    sealed class ThrowingHandler : IFieldHandler
    {
        public string Name => "Throwing";
        public byte[]? Configuration => null;
        public bool IsCompatibleWith(Type type, FieldHandlerOptions options) => false;
        public Type? HandledType() => throw new InvalidOperationException();
        public bool NeedsCtx() => false;
        public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx) => throw new NotSupportedException();
        public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx) => throw new NotSupportedException();

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx,
            Action<IILGen> pushValue) => throw new NotSupportedException();

        public FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory) => throw new NotSupportedException();
        public void Skip(ref MemReader reader, IReaderCtx? ctx) => throw new NotSupportedException();
        public FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory) => throw new NotSupportedException();
        public void FreeContent(ref MemReader reader, IReaderCtx? ctx) => throw new NotSupportedException();
        public bool DoesNeedFreeContent(HashSet<Type> visitedTypes) => false;

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger) =>
            this;

        public IFieldHandler SpecializeSaveForType(Type type) => this;
    }
}
