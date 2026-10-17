using BTDB.StreamLayer;

namespace DBBenchmark;

// Benchmark-only prototype of the alternative to the bytecode interpreter: a tree of sealed nodes with virtual
// dispatch. It is AOT friendly like the interpreter (no code generation) and exists only to measure the dispatch cost
// difference. It simplifies on purpose: one value version, numbers widened to long, no type conversions.

sealed class ClosureFrame
{
    public readonly long[] Numbers = new long[8];
    public readonly string?[] Strings = new string?[8];
}

abstract class ClosureFieldStep
{
    public abstract void Run(ref MemReader reader, ClosureFrame frame);
}

sealed class ClosureSkipStringStep : ClosureFieldStep
{
    public override void Run(ref MemReader reader, ClosureFrame frame) => reader.SkipString();
}

sealed class ClosureSkipVInt64Step : ClosureFieldStep
{
    public override void Run(ref MemReader reader, ClosureFrame frame) => reader.SkipVInt64();
}

sealed class ClosureReadStringStep(int slot) : ClosureFieldStep
{
    public override void Run(ref MemReader reader, ClosureFrame frame) => frame.Strings[slot] = reader.ReadString();
}

sealed class ClosureReadVInt64Step(int slot) : ClosureFieldStep
{
    public override void Run(ref MemReader reader, ClosureFrame frame) => frame.Numbers[slot] = reader.ReadVInt64();
}

sealed class ClosureReadBoolStep(int slot) : ClosureFieldStep
{
    public override void Run(ref MemReader reader, ClosureFrame frame) =>
        frame.Numbers[slot] = reader.ReadBool() ? 1 : 0;
}

abstract class ClosurePredicate
{
    public abstract bool Eval(ClosureFrame frame);
}

sealed class ClosureAnd(ClosurePredicate left, ClosurePredicate right) : ClosurePredicate
{
    public override bool Eval(ClosureFrame frame) => left.Eval(frame) && right.Eval(frame);
}

sealed class ClosureEqualNumber(int slot, long value) : ClosurePredicate
{
    public override bool Eval(ClosureFrame frame) => frame.Numbers[slot] == value;
}

sealed class ClosureEqualString(int slot, string? value) : ClosurePredicate
{
    public override bool Eval(ClosureFrame frame) => frame.Strings[slot] == value;
}

sealed class ClosureProgram(ClosureFieldStep[] steps, ClosurePredicate predicate)
{
    public bool Evaluate(ref MemReader valueReader, ClosureFrame frame)
    {
        valueReader.SkipVUInt32();
        foreach (var step in steps) step.Run(ref valueReader, frame);
        return predicate.Eval(frame);
    }
}
