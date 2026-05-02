using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.Buffer;

public static class RoaringBitmaps
{
    public const int BitmapSize = 8192;
    const int BitCount = BitmapSize * 8;

    enum ContainerKind
    {
        Empty,
        Array,
        Rle,
        Bitmap
    }

    enum BinaryOperation
    {
        And,
        Or,
        AndNot
    }

    enum OutputKind
    {
        Array,
        Rle,
        Bitmap
    }

    public static void SetBit(Span<byte> bitmap, ushort offset)
    {
        EnsureBitmapSize(bitmap);
        bitmap[offset >> 3] |= (byte)(1u << (offset & 7));
    }

    public static void UnsetBit(Span<byte> bitmap, ushort offset)
    {
        EnsureBitmapSize(bitmap);
        bitmap[offset >> 3] &= (byte)~(1u << (offset & 7));
    }

    public static bool GetBit(ReadOnlySpan<byte> bitmap, ushort offset)
    {
        EnsureBitmapSize(bitmap);
        return (bitmap[offset >> 3] & (1u << (offset & 7))) != 0;
    }

    public static bool Contains(ReadOnlySpan<byte> bitmap, ushort offset)
    {
        return GetBitEncoded(bitmap, offset);
    }

    public static int Compress(ReadOnlySpan<byte> bitmap, Span<byte> output)
    {
        EnsureBitmapSize(bitmap);
        EnsureOutputSize(output);

        if (bitmap.Overlaps(output))
        {
            Span<byte> source = stackalloc byte[BitmapSize];
            bitmap.CopyTo(source);
            return CompressNoOverlap(source, output);
        }

        return CompressNoOverlap(bitmap, output);
    }

    static int CompressNoOverlap(ReadOnlySpan<byte> bitmap, Span<byte> output)
    {
        var stats = GetBitmapStats(bitmap);
        return WriteBitmapUsingBestEncoding(bitmap, output, stats.Cardinality, stats.Runs);
    }

    public static void ToBitmap(ReadOnlySpan<byte> input, Span<byte> output)
    {
        EnsureOutputSize(output);
        output.Clear();
        switch (ValidateAndGetKind(input))
        {
            case ContainerKind.Empty:
                return;
            case ContainerKind.Bitmap:
                input.CopyTo(output);
                return;
            case ContainerKind.Array:
                for (var i = 0; i < input.Length; i += 2)
                    SetBitUnchecked(output, ReadUInt16(input, i));
                return;
            case ContainerKind.Rle:
                for (var i = 0; i < input.Length - 1; i += 4)
                {
                    var start = ReadUInt16(input, i);
                    var end = start + ReadUInt16(input, i + 2);
                    SetRange(output, start, end);
                }

                return;
        }
    }

    public static int And(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> output)
    {
        return Operate(left, right, output, BinaryOperation.And);
    }

    public static int Or(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> output)
    {
        return Operate(left, right, output, BinaryOperation.Or);
    }

    public static int AndNot(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> output)
    {
        return Operate(left, right, output, BinaryOperation.AndNot);
    }

    public static int Not(ReadOnlySpan<byte> input, Span<byte> output)
    {
        EnsureOutputSize(output);
        var kind = ValidateAndGetKind(input);
        if (kind == ContainerKind.Empty)
            return WriteRleRun(output, 0, ushort.MaxValue);

        Span<byte> bitmap = stackalloc byte[BitmapSize];
        if (kind == ContainerKind.Bitmap)
            InvertBitmap(input, bitmap);
        else
        {
            ToBitmap(input, bitmap);
            InvertBitmap(bitmap, bitmap);
        }

        return Compress(bitmap, output);
    }

    public static void Not(Span<byte> bitmap)
    {
        EnsureBitmapSize(bitmap);
        InvertBitmap(bitmap, bitmap);
    }

    public static void And(Span<byte> bitmap, ReadOnlySpan<byte> input)
    {
        EnsureBitmapSize(bitmap);
        var kind = ValidateAndGetKind(input);
        if (kind == ContainerKind.Empty)
        {
            bitmap.Clear();
            return;
        }

        if (kind == ContainerKind.Bitmap)
        {
            AndBitmaps(bitmap, input, bitmap);
            return;
        }

        AndIntoBitmap(bitmap, input, kind);
    }

    public static void Or(Span<byte> bitmap, ReadOnlySpan<byte> input)
    {
        EnsureBitmapSize(bitmap);
        var kind = ValidateAndGetKind(input);
        if (kind == ContainerKind.Empty) return;

        if (kind == ContainerKind.Bitmap)
        {
            OrBitmaps(bitmap, input, bitmap);
            return;
        }

        OrIntoBitmap(bitmap, input, kind);
    }

    public static void OrNot(Span<byte> bitmap, ReadOnlySpan<byte> input)
    {
        EnsureBitmapSize(bitmap);
        var kind = ValidateAndGetKind(input);
        if (kind == ContainerKind.Empty)
        {
            bitmap.Fill(0xff);
            return;
        }

        if (kind == ContainerKind.Bitmap)
        {
            OrNotBitmaps(bitmap, input, bitmap);
            return;
        }

        OrNotIntoBitmap(bitmap, input, kind);
    }

    public static void AndNot(Span<byte> bitmap, ReadOnlySpan<byte> input)
    {
        EnsureBitmapSize(bitmap);
        var kind = ValidateAndGetKind(input);
        if (kind == ContainerKind.Empty) return;

        if (kind == ContainerKind.Bitmap)
        {
            AndNotBitmaps(bitmap, input, bitmap);
            return;
        }

        AndNotIntoBitmap(bitmap, input, kind);
    }

    public static RoaringBitmapEnumerable Enumerate(ReadOnlyMemory<byte> input, ulong addOffset)
    {
        ValidateAndGetKind(input.Span);
        return new(input, addOffset);
    }

    public static DenseBitmapEnumerable EnumerateBitmap(ReadOnlySpan<byte> bitmap, ulong addOffset)
    {
        EnsureBitmapSize(bitmap);
        return new(bitmap, addOffset);
    }

    static int Operate(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> output, BinaryOperation operation)
    {
        EnsureOutputSize(output);
        var leftKind = ValidateAndGetKind(left);
        var rightKind = ValidateAndGetKind(right);

        if (operation == BinaryOperation.And)
        {
            if (leftKind == ContainerKind.Empty || rightKind == ContainerKind.Empty) return 0;
        }
        else if (operation == BinaryOperation.Or)
        {
            if (leftKind == ContainerKind.Empty) return CopyEncoded(right, rightKind, output);
            if (rightKind == ContainerKind.Empty) return CopyEncoded(left, leftKind, output);
        }
        else
        {
            if (leftKind == ContainerKind.Empty) return 0;
            if (rightKind == ContainerKind.Empty) return CopyEncoded(left, leftKind, output);
        }

        if (leftKind == ContainerKind.Array && rightKind == ContainerKind.Array &&
            operation != BinaryOperation.Or)
            return OperateArrays(left, right, output, operation);

        if (leftKind != ContainerKind.Bitmap && rightKind != ContainerKind.Bitmap)
            return OperateIntervals(left, leftKind, right, rightKind, output, operation);

        if (leftKind == ContainerKind.Bitmap && rightKind == ContainerKind.Bitmap)
            return OperateBitmaps(left, right, output, operation);

        return OperateWithBitmap(left, leftKind, right, rightKind, output, operation);
    }

    static int OperateArrays(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> output,
        BinaryOperation operation)
    {
        return operation == BinaryOperation.And
            ? WriteArrayAnd(left, right, output)
            : WriteArrayAndNot(left, right, output);
    }

    static int WriteArrayAnd(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> output)
    {
        var leftOffset = 0;
        var rightOffset = 0;
        var outputOffset = 0;
        while (leftOffset < left.Length && rightOffset < right.Length)
        {
            var leftValue = ReadUInt16(left, leftOffset);
            var rightValue = ReadUInt16(right, rightOffset);
            if (leftValue == rightValue)
            {
                WriteUInt16(output, outputOffset, leftValue);
                outputOffset += 2;
                leftOffset += 2;
                rightOffset += 2;
            }
            else if (leftValue < rightValue)
                leftOffset += 2;
            else
                rightOffset += 2;
        }

        return outputOffset;
    }

    static int WriteArrayAndNot(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> output)
    {
        var leftOffset = 0;
        var rightOffset = 0;
        var outputOffset = 0;
        while (leftOffset < left.Length)
        {
            var leftValue = ReadUInt16(left, leftOffset);
            while (rightOffset < right.Length && ReadUInt16(right, rightOffset) < leftValue)
                rightOffset += 2;
            if (rightOffset == right.Length || ReadUInt16(right, rightOffset) != leftValue)
            {
                WriteUInt16(output, outputOffset, leftValue);
                outputOffset += 2;
            }

            leftOffset += 2;
        }

        return outputOffset;
    }

    static int CopyEncoded(ReadOnlySpan<byte> input, ContainerKind kind, Span<byte> output)
    {
        if (kind != ContainerKind.Bitmap)
        {
            input.CopyTo(output);
            return input.Length;
        }

        input.CopyTo(output);
        return BitmapSize;
    }

    static int OperateIntervals(ReadOnlySpan<byte> left, ContainerKind leftKind, ReadOnlySpan<byte> right,
        ContainerKind rightKind, Span<byte> output, BinaryOperation operation)
    {
        var counter = new IntervalResultBuilder(default, OutputKind.Array);
        RunIntervalOperation(left, leftKind, right, rightKind, ref counter, operation);
        counter.Complete();
        if (counter.Cardinality == 0) return 0;

        var outputKind = ChooseOutputKind(counter.Cardinality, counter.Runs);
        var writer = new IntervalResultBuilder(output, outputKind);
        RunIntervalOperation(left, leftKind, right, rightKind, ref writer, operation);
        writer.Complete();
        return LengthFor(outputKind, writer.Cardinality, writer.Runs);
    }

    static int OperateWithBitmap(ReadOnlySpan<byte> left, ContainerKind leftKind, ReadOnlySpan<byte> right,
        ContainerKind rightKind, Span<byte> output, BinaryOperation operation)
    {
        if (operation == BinaryOperation.And && leftKind == ContainerKind.Array && rightKind == ContainerKind.Bitmap)
            return FilterArrayWithBitmap(left, right, output, false);
        if (operation == BinaryOperation.And && leftKind == ContainerKind.Bitmap && rightKind == ContainerKind.Array)
            return FilterArrayWithBitmap(right, left, output, false);
        if (operation == BinaryOperation.AndNot && leftKind == ContainerKind.Array && rightKind == ContainerKind.Bitmap)
            return FilterArrayWithBitmap(left, right, output, true);

        Span<byte> bitmap = stackalloc byte[BitmapSize];
        if (operation == BinaryOperation.And)
        {
            bitmap.Clear();
            if (leftKind == ContainerKind.Bitmap)
            {
                ToBitmap(right, bitmap);
                return OperateBitmaps(left, bitmap, output, BinaryOperation.And);
            }

            ToBitmap(left, bitmap);
            return OperateBitmaps(bitmap, right, output, BinaryOperation.And);
        }

        if (operation == BinaryOperation.Or)
        {
            if (leftKind == ContainerKind.Bitmap)
            {
                left.CopyTo(bitmap);
                OrIntoBitmap(bitmap, right, rightKind);
            }
            else
            {
                right.CopyTo(bitmap);
                OrIntoBitmap(bitmap, left, leftKind);
            }

            return Compress(bitmap, output);
        }

        if (leftKind == ContainerKind.Bitmap)
        {
            left.CopyTo(bitmap);
            AndNotIntoBitmap(bitmap, right, rightKind);
            return Compress(bitmap, output);
        }

        ToBitmap(left, bitmap);
        return OperateBitmaps(bitmap, right, output, BinaryOperation.AndNot);
    }

    static int FilterArrayWithBitmap(ReadOnlySpan<byte> array, ReadOnlySpan<byte> bitmap, Span<byte> output,
        bool invert)
    {
        var counter = new IntervalResultBuilder(default, OutputKind.Array);
        for (var i = 0; i < array.Length; i += 2)
        {
            var value = ReadUInt16(array, i);
            if (GetBitUnchecked(bitmap, value) != invert) counter.AddInterval(value, value);
        }

        counter.Complete();
        if (counter.Cardinality == 0) return 0;

        var outputKind = ChooseOutputKind(counter.Cardinality, counter.Runs);
        var writer = new IntervalResultBuilder(output, outputKind);
        for (var i = 0; i < array.Length; i += 2)
        {
            var value = ReadUInt16(array, i);
            if (GetBitUnchecked(bitmap, value) != invert) writer.AddInterval(value, value);
        }

        writer.Complete();
        return LengthFor(outputKind, writer.Cardinality, writer.Runs);
    }

    static int OperateBitmaps(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> output,
        BinaryOperation operation)
    {
        Span<byte> bitmap = stackalloc byte[BitmapSize];
        if (operation == BinaryOperation.And)
            AndBitmaps(left, right, bitmap);
        else if (operation == BinaryOperation.Or)
            OrBitmaps(left, right, bitmap);
        else
            AndNotBitmaps(left, right, bitmap);

        return Compress(bitmap, output);
    }

    static unsafe void AndBitmaps(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> output)
    {
        var vectorSize = Vector<byte>.Count;
        fixed (byte* leftPtr = left)
        fixed (byte* rightPtr = right)
        fixed (byte* outputPtr = output)
        {
            if (AreAligned(vectorSize, leftPtr, rightPtr, outputPtr))
            {
                for (var i = 0; i < BitmapSize; i += vectorSize)
                    Unsafe.Write(outputPtr + i,
                        Unsafe.Read<Vector<byte>>(leftPtr + i) &
                        Unsafe.Read<Vector<byte>>(rightPtr + i));
                return;
            }

            for (var i = 0; i < BitmapSize; i += vectorSize)
                Unsafe.WriteUnaligned(outputPtr + i,
                    Unsafe.ReadUnaligned<Vector<byte>>(leftPtr + i) &
                    Unsafe.ReadUnaligned<Vector<byte>>(rightPtr + i));
        }
    }

    static unsafe void OrBitmaps(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> output)
    {
        var vectorSize = Vector<byte>.Count;
        fixed (byte* leftPtr = left)
        fixed (byte* rightPtr = right)
        fixed (byte* outputPtr = output)
        {
            if (AreAligned(vectorSize, leftPtr, rightPtr, outputPtr))
            {
                for (var i = 0; i < BitmapSize; i += vectorSize)
                    Unsafe.Write(outputPtr + i,
                        Unsafe.Read<Vector<byte>>(leftPtr + i) |
                        Unsafe.Read<Vector<byte>>(rightPtr + i));
                return;
            }

            for (var i = 0; i < BitmapSize; i += vectorSize)
                Unsafe.WriteUnaligned(outputPtr + i,
                    Unsafe.ReadUnaligned<Vector<byte>>(leftPtr + i) |
                    Unsafe.ReadUnaligned<Vector<byte>>(rightPtr + i));
        }
    }

    static unsafe void AndNotBitmaps(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> output)
    {
        var vectorSize = Vector<byte>.Count;
        fixed (byte* leftPtr = left)
        fixed (byte* rightPtr = right)
        fixed (byte* outputPtr = output)
        {
            if (AreAligned(vectorSize, leftPtr, rightPtr, outputPtr))
            {
                for (var i = 0; i < BitmapSize; i += vectorSize)
                    Unsafe.Write(outputPtr + i,
                        Unsafe.Read<Vector<byte>>(leftPtr + i) &
                        ~Unsafe.Read<Vector<byte>>(rightPtr + i));
                return;
            }

            for (var i = 0; i < BitmapSize; i += vectorSize)
                Unsafe.WriteUnaligned(outputPtr + i,
                    Unsafe.ReadUnaligned<Vector<byte>>(leftPtr + i) &
                    ~Unsafe.ReadUnaligned<Vector<byte>>(rightPtr + i));
        }
    }

    static unsafe void OrNotBitmaps(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, Span<byte> output)
    {
        var vectorSize = Vector<byte>.Count;
        fixed (byte* leftPtr = left)
        fixed (byte* rightPtr = right)
        fixed (byte* outputPtr = output)
        {
            if (AreAligned(vectorSize, leftPtr, rightPtr, outputPtr))
            {
                for (var i = 0; i < BitmapSize; i += vectorSize)
                    Unsafe.Write(outputPtr + i,
                        Unsafe.Read<Vector<byte>>(leftPtr + i) |
                        ~Unsafe.Read<Vector<byte>>(rightPtr + i));
                return;
            }

            for (var i = 0; i < BitmapSize; i += vectorSize)
                Unsafe.WriteUnaligned(outputPtr + i,
                    Unsafe.ReadUnaligned<Vector<byte>>(leftPtr + i) |
                    ~Unsafe.ReadUnaligned<Vector<byte>>(rightPtr + i));
        }
    }

    static void RunIntervalOperation(ReadOnlySpan<byte> left, ContainerKind leftKind, ReadOnlySpan<byte> right,
        ContainerKind rightKind, ref IntervalResultBuilder builder, BinaryOperation operation)
    {
        if (operation == BinaryOperation.And)
        {
            RunIntervalAnd(left, leftKind, right, rightKind, ref builder);
            return;
        }

        if (operation == BinaryOperation.Or)
            RunIntervalOr(left, leftKind, right, rightKind, ref builder);
        else
            RunIntervalAndNot(left, leftKind, right, rightKind, ref builder);
    }

    static void RunIntervalAnd(ReadOnlySpan<byte> left, ContainerKind leftKind, ReadOnlySpan<byte> right,
        ContainerKind rightKind, ref IntervalResultBuilder builder)
    {
        var leftEnumerator = new IntervalEnumerator(left, leftKind);
        var rightEnumerator = new IntervalEnumerator(right, rightKind);
        var hasLeft = leftEnumerator.MoveNext();
        var hasRight = rightEnumerator.MoveNext();
        while (hasLeft && hasRight)
        {
            var start = Math.Max(leftEnumerator.Start, rightEnumerator.Start);
            var end = Math.Min(leftEnumerator.End, rightEnumerator.End);
            if (start <= end) builder.AddInterval((ushort)start, (ushort)end);
            if (leftEnumerator.End < rightEnumerator.End)
                hasLeft = leftEnumerator.MoveNext();
            else
                hasRight = rightEnumerator.MoveNext();
        }
    }

    static void RunIntervalOr(ReadOnlySpan<byte> left, ContainerKind leftKind, ReadOnlySpan<byte> right,
        ContainerKind rightKind, ref IntervalResultBuilder builder)
    {
        var leftEnumerator = new IntervalEnumerator(left, leftKind);
        var rightEnumerator = new IntervalEnumerator(right, rightKind);
        var hasLeft = leftEnumerator.MoveNext();
        var hasRight = rightEnumerator.MoveNext();
        while (hasLeft || hasRight)
        {
            if (!hasRight || hasLeft && leftEnumerator.Start <= rightEnumerator.Start)
            {
                builder.AddInterval(leftEnumerator.Start, leftEnumerator.End);
                hasLeft = leftEnumerator.MoveNext();
            }
            else
            {
                builder.AddInterval(rightEnumerator.Start, rightEnumerator.End);
                hasRight = rightEnumerator.MoveNext();
            }
        }
    }

    static void RunIntervalAndNot(ReadOnlySpan<byte> left, ContainerKind leftKind, ReadOnlySpan<byte> right,
        ContainerKind rightKind, ref IntervalResultBuilder builder)
    {
        var leftEnumerator = new IntervalEnumerator(left, leftKind);
        var rightEnumerator = new IntervalEnumerator(right, rightKind);
        var hasRight = rightEnumerator.MoveNext();
        while (leftEnumerator.MoveNext())
        {
            var start = (uint)leftEnumerator.Start;
            var end = (uint)leftEnumerator.End;
            while (hasRight && rightEnumerator.End < start)
                hasRight = rightEnumerator.MoveNext();

            var current = start;
            while (hasRight && rightEnumerator.Start <= end)
            {
                if (rightEnumerator.Start > current)
                    builder.AddInterval((ushort)current, (ushort)(rightEnumerator.Start - 1));

                if (rightEnumerator.End == ushort.MaxValue)
                {
                    current = BitCount;
                    break;
                }

                current = Math.Max(current, (uint)rightEnumerator.End + 1);
                if (current > end) break;
                hasRight = rightEnumerator.MoveNext();
            }

            if (current <= end)
                builder.AddInterval((ushort)current, (ushort)end);
        }
    }

    static void OrIntoBitmap(Span<byte> bitmap, ReadOnlySpan<byte> input, ContainerKind kind)
    {
        if (kind == ContainerKind.Array)
        {
            for (var i = 0; i < input.Length; i += 2)
                SetBitUnchecked(bitmap, ReadUInt16(input, i));
            return;
        }

        for (var i = 0; i < input.Length - 1; i += 4)
            SetRange(bitmap, ReadUInt16(input, i), ReadUInt16(input, i) + ReadUInt16(input, i + 2));
    }

    static void AndNotIntoBitmap(Span<byte> bitmap, ReadOnlySpan<byte> input, ContainerKind kind)
    {
        if (kind == ContainerKind.Array)
        {
            for (var i = 0; i < input.Length; i += 2)
                UnsetBitUnchecked(bitmap, ReadUInt16(input, i));
            return;
        }

        for (var i = 0; i < input.Length - 1; i += 4)
            ClearRange(bitmap, ReadUInt16(input, i), ReadUInt16(input, i) + ReadUInt16(input, i + 2));
    }

    static void AndIntoBitmap(Span<byte> bitmap, ReadOnlySpan<byte> input, ContainerKind kind)
    {
        var clearStart = 0;
        var enumerator = new IntervalEnumerator(input, kind);
        while (enumerator.MoveNext())
        {
            if (clearStart < enumerator.Start)
                ClearRange(bitmap, clearStart, enumerator.Start - 1);
            if (enumerator.End == ushort.MaxValue) return;
            clearStart = enumerator.End + 1;
        }

        if (clearStart < BitCount)
            ClearRange(bitmap, clearStart, BitCount - 1);
    }

    static void OrNotIntoBitmap(Span<byte> bitmap, ReadOnlySpan<byte> input, ContainerKind kind)
    {
        var setStart = 0;
        var enumerator = new IntervalEnumerator(input, kind);
        while (enumerator.MoveNext())
        {
            if (setStart < enumerator.Start)
                SetRange(bitmap, setStart, enumerator.Start - 1);
            if (enumerator.End == ushort.MaxValue) return;
            setStart = enumerator.End + 1;
        }

        if (setStart < BitCount)
            SetRange(bitmap, setStart, BitCount - 1);
    }

    static (int Cardinality, int Runs) GetBitmapStats(ReadOnlySpan<byte> bitmap)
    {
        var cardinality = 0;
        var runs = 0;
        var previousHighBit = 0ul;
        for (var i = 0; i < BitmapSize; i += sizeof(ulong))
        {
            var value = ReadUInt64LittleEndianUnsafe(bitmap, i);
            cardinality += BitOperations.PopCount(value);
            runs += BitOperations.PopCount(value & ~(value << 1));
            runs -= (int)(previousHighBit & value & 1);
            previousHighBit = value >> 63;
        }

        return (cardinality, runs);
    }

    static int WriteBitmapUsingBestEncoding(ReadOnlySpan<byte> bitmap, Span<byte> output, int cardinality, int runs)
    {
        if (cardinality == 0) return 0;
        var outputKind = ChooseOutputKind(cardinality, runs);
        if (outputKind == OutputKind.Array) return WriteArrayFromBitmap(bitmap, output);
        if (outputKind == OutputKind.Rle) return WriteRleFromBitmap(bitmap, output);
        bitmap.CopyTo(output);
        return BitmapSize;
    }

    static OutputKind ChooseOutputKind(int cardinality, int runs)
    {
        var bestKind = OutputKind.Bitmap;
        var bestLength = BitmapSize;

        var arrayLength = cardinality * 2;
        if (arrayLength < bestLength)
        {
            bestLength = arrayLength;
            bestKind = OutputKind.Array;
        }

        var rleLength = runs * 4 + 1;
        if (rleLength < bestLength)
            bestKind = OutputKind.Rle;

        return bestKind;
    }

    static int LengthFor(OutputKind kind, int cardinality, int runs)
    {
        if (kind == OutputKind.Array) return cardinality * 2;
        if (kind == OutputKind.Rle) return runs * 4 + 1;
        return BitmapSize;
    }

    static unsafe int WriteArrayFromBitmap(ReadOnlySpan<byte> bitmap, Span<byte> output)
    {
        fixed (byte* bitmapPtr = bitmap)
        fixed (byte* outputPtr = output)
        {
            var outputUshortPtr = (ushort*)outputPtr;
            for (var wordIndex = 0; wordIndex < BitmapSize / sizeof(ulong); wordIndex++)
            {
                var value = Unsafe.ReadUnaligned<ulong>(bitmapPtr + wordIndex * sizeof(ulong));
                while (value != 0)
                {
                    var bit = BitOperations.TrailingZeroCount(value);
                    *outputUshortPtr++ = (ushort)(wordIndex * 64 + bit);
                    value &= value - 1;
                }
            }

            return (int)((byte*)outputUshortPtr - outputPtr);
        }
    }

    static int WriteRleFromBitmap(ReadOnlySpan<byte> bitmap, Span<byte> output)
    {
        var offset = 0;
        var pendingStart = -1;
        var pendingEnd = -1;
        for (var wordIndex = 0; wordIndex < BitmapSize / sizeof(ulong); wordIndex++)
        {
            var value = ReadUInt64LittleEndianUnsafe(bitmap, wordIndex * sizeof(ulong));
            while (value != 0)
            {
                var bit = BitOperations.TrailingZeroCount(value);
                var shifted = value >> bit;
                var length = BitOperations.TrailingZeroCount(~shifted);
                if (length == 64) length -= bit;
                var start = wordIndex * 64 + bit;
                var end = start + length - 1;
                if (pendingStart < 0)
                {
                    pendingStart = start;
                    pendingEnd = end;
                }
                else if (start == pendingEnd + 1)
                    pendingEnd = end;
                else
                {
                    WriteUInt16(output, offset, pendingStart);
                    WriteUInt16(output, offset + 2, pendingEnd - pendingStart);
                    offset += 4;
                    pendingStart = start;
                    pendingEnd = end;
                }

                var lowestBit = value & (0ul - value);
                value &= value + lowestBit;
            }
        }

        if (pendingStart >= 0)
        {
            WriteUInt16(output, offset, pendingStart);
            WriteUInt16(output, offset + 2, pendingEnd - pendingStart);
            offset += 4;
        }

        output[offset++] = 0;
        return offset;
    }

    static int WriteRleRun(Span<byte> output, ushort start, ushort lengthMinusOne)
    {
        WriteUInt16(output, 0, start);
        WriteUInt16(output, 2, lengthMinusOne);
        output[4] = 0;
        return 5;
    }

    static unsafe void InvertBitmap(ReadOnlySpan<byte> input, Span<byte> output)
    {
        var vectorSize = Vector<byte>.Count;
        var allBits = new Vector<byte>(0xff);
        fixed (byte* inputPtr = input)
        fixed (byte* outputPtr = output)
        {
            if (AreAligned(vectorSize, inputPtr, outputPtr))
            {
                for (var i = 0; i < BitmapSize; i += vectorSize)
                    Unsafe.Write(outputPtr + i, Unsafe.Read<Vector<byte>>(inputPtr + i) ^ allBits);
                return;
            }

            for (var i = 0; i < BitmapSize; i += vectorSize)
                Unsafe.WriteUnaligned(outputPtr + i,
                    Unsafe.ReadUnaligned<Vector<byte>>(inputPtr + i) ^ allBits);
        }
    }

    static bool GetBitEncoded(ReadOnlySpan<byte> input, ushort offset)
    {
        var kind = ValidateAndGetKind(input);
        if (kind == ContainerKind.Empty) return false;
        if (kind == ContainerKind.Bitmap) return GetBitUnchecked(input, offset);
        if (kind == ContainerKind.Array) return ArrayContains(input, offset);
        return RleContains(input, offset);
    }

    static bool ArrayContains(ReadOnlySpan<byte> input, ushort value)
    {
        var lo = 0;
        var hi = input.Length / 2 - 1;
        while (lo <= hi)
        {
            var mid = (lo + hi) >> 1;
            var current = ReadUInt16(input, mid * 2);
            if (current == value) return true;
            if (current < value) lo = mid + 1;
            else hi = mid - 1;
        }

        return false;
    }

    static bool RleContains(ReadOnlySpan<byte> input, ushort value)
    {
        var lo = 0;
        var hi = (input.Length - 1) / 4 - 1;
        while (lo <= hi)
        {
            var mid = (lo + hi) >> 1;
            var offset = mid * 4;
            var start = ReadUInt16(input, offset);
            var end = start + ReadUInt16(input, offset + 2);
            if (value < start) hi = mid - 1;
            else if (value > end) lo = mid + 1;
            else return true;
        }

        return false;
    }

    static ContainerKind ValidateAndGetKind(ReadOnlySpan<byte> input)
    {
        if (input.Length == 0) return ContainerKind.Empty;
        if (input.Length > BitmapSize)
            throw new ArgumentException("Encoded container length must not exceed 8192 bytes.", nameof(input));
        if (input.Length == BitmapSize) return ContainerKind.Bitmap;

        if ((input.Length & 1) == 0)
        {
            ValidateArray(input);
            return ContainerKind.Array;
        }

        ValidateRle(input);
        return ContainerKind.Rle;
    }

    static void ValidateArray(ReadOnlySpan<byte> input)
    {
        ushort previous = 0;
        for (var i = 0; i < input.Length; i += 2)
        {
            var value = ReadUInt16(input, i);
            if (i != 0 && value <= previous)
                throw new ArgumentException("Array container values must be strictly sorted.", nameof(input));
            previous = value;
        }
    }

    static void ValidateRle(ReadOnlySpan<byte> input)
    {
        if (input.Length < 5 || (input.Length & 3) != 1 || input[^1] != 0)
            throw new ArgumentException("RLE container length must be 4 * runCount + 1 and end with zero padding.",
                nameof(input));

        uint previousEnd = 0;
        for (var i = 0; i < input.Length - 1; i += 4)
        {
            var start = ReadUInt16(input, i);
            var lengthMinusOne = ReadUInt16(input, i + 2);
            var end = (uint)start + lengthMinusOne;
            if (end >= BitCount)
                throw new ArgumentException("RLE run exceeds 16-bit bitmap range.", nameof(input));
            if (i != 0 && start <= previousEnd)
                throw new ArgumentException("RLE runs must be sorted and non-overlapping.", nameof(input));
            previousEnd = end;
        }
    }

    static void EnsureBitmapSize(ReadOnlySpan<byte> bitmap)
    {
        if (bitmap.Length != BitmapSize)
            throw new ArgumentException("Raw bitmap must be exactly 8192 bytes.", nameof(bitmap));
    }

    static void EnsureOutputSize(Span<byte> output)
    {
        if (output.Length < BitmapSize)
            throw new ArgumentException("Output buffer must be at least 8192 bytes.", nameof(output));
    }

    static ushort ReadUInt16(ReadOnlySpan<byte> input, int offset)
    {
        return ReadUInt16LittleEndianUnsafe(input, offset);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ushort ReadUInt16LittleEndianUnsafe(ReadOnlySpan<byte> input, int offset)
    {
        return PackUnpack.AsLittleEndian(
            Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref MemoryMarshal.GetReference(input), offset)));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ulong ReadUInt64LittleEndianUnsafe(ReadOnlySpan<byte> input, int offset)
    {
        return PackUnpack.AsLittleEndian(
            Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref MemoryMarshal.GetReference(input), offset)));
    }

    static void WriteUInt16(Span<byte> output, int offset, int value)
    {
        Unsafe.WriteUnaligned(ref Unsafe.Add(ref MemoryMarshal.GetReference(output), offset),
            PackUnpack.AsLittleEndian((ushort)value));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static unsafe bool AreAligned(int alignment, byte* first, byte* second)
    {
        return ((((nuint)first | (nuint)second) & (nuint)(alignment - 1)) == 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static unsafe bool AreAligned(int alignment, byte* first, byte* second, byte* third)
    {
        return ((((nuint)first | (nuint)second | (nuint)third) & (nuint)(alignment - 1)) == 0);
    }

    static void SetBitUnchecked(Span<byte> bitmap, int offset)
    {
        bitmap[offset >> 3] |= (byte)(1u << (offset & 7));
    }

    static void UnsetBitUnchecked(Span<byte> bitmap, int offset)
    {
        bitmap[offset >> 3] &= (byte)~(1u << (offset & 7));
    }

    static bool GetBitUnchecked(ReadOnlySpan<byte> bitmap, int offset)
    {
        return (bitmap[offset >> 3] & (1u << (offset & 7))) != 0;
    }

    static void SetRange(Span<byte> bitmap, int start, int end)
    {
        ChangeRange(bitmap, start, end, true);
    }

    static void ClearRange(Span<byte> bitmap, int start, int end)
    {
        ChangeRange(bitmap, start, end, false);
    }

    static void ChangeRange(Span<byte> bitmap, int start, int end, bool set)
    {
        while (start <= end && (start & 7) != 0)
        {
            if (set) SetBitUnchecked(bitmap, start);
            else UnsetBitUnchecked(bitmap, start);
            start++;
        }

        var byteCount = (end - start + 1) >> 3;
        if (byteCount != 0)
        {
            bitmap.Slice(start >> 3, byteCount).Fill(set ? (byte)0xff : (byte)0);
            start += byteCount << 3;
        }

        while (start <= end)
        {
            if (set) SetBitUnchecked(bitmap, start);
            else UnsetBitUnchecked(bitmap, start);
            start++;
        }
    }

    ref struct IntervalEnumerator
    {
        readonly ReadOnlySpan<byte> _input;
        readonly ContainerKind _kind;
        int _index;

        public ushort Start { get; private set; }
        public ushort End { get; private set; }

        public IntervalEnumerator(ReadOnlySpan<byte> input, ContainerKind kind)
        {
            _input = input;
            _kind = kind;
            _index = 0;
            Start = 0;
            End = 0;
        }

        public bool MoveNext()
        {
            if (_kind == ContainerKind.Array)
            {
                if (_index >= _input.Length) return false;
                Start = ReadUInt16(_input, _index);
                End = Start;
                _index += 2;
                return true;
            }

            if (_index >= _input.Length - 1) return false;
            Start = ReadUInt16(_input, _index);
            End = (ushort)(Start + ReadUInt16(_input, _index + 2));
            _index += 4;
            return true;
        }
    }

    ref struct IntervalResultBuilder
    {
        readonly Span<byte> _output;
        readonly OutputKind _outputKind;
        bool _hasPending;
        ushort _pendingStart;
        ushort _pendingEnd;
        int _offset;

        public int Cardinality { get; private set; }
        public int Runs { get; private set; }

        public IntervalResultBuilder(Span<byte> output, OutputKind outputKind)
        {
            _output = output;
            _outputKind = outputKind;
            _hasPending = false;
            _pendingStart = 0;
            _pendingEnd = 0;
            _offset = 0;
            Cardinality = 0;
            Runs = 0;
            if (outputKind == OutputKind.Bitmap && output.Length != 0)
                output.Slice(0, BitmapSize).Clear();
        }

        public void AddInterval(ushort start, ushort end)
        {
            if (!_hasPending)
            {
                _pendingStart = start;
                _pendingEnd = end;
                _hasPending = true;
                return;
            }

            if ((uint)start <= (uint)_pendingEnd + 1)
            {
                if (end > _pendingEnd) _pendingEnd = end;
                return;
            }

            FlushPending();
            _pendingStart = start;
            _pendingEnd = end;
            _hasPending = true;
        }

        public void Complete()
        {
            FlushPending();
            if (_outputKind == OutputKind.Rle && _output.Length != 0)
                _output[_offset] = 0;
        }

        void FlushPending()
        {
            if (!_hasPending) return;
            Cardinality += _pendingEnd - _pendingStart + 1;
            Runs++;
            if (_outputKind == OutputKind.Array)
            {
                if (_output.Length != 0)
                {
                    for (var i = _pendingStart; i <= _pendingEnd; i++)
                    {
                        WriteUInt16(_output, _offset, i);
                        _offset += 2;
                        if (i == ushort.MaxValue) break;
                    }
                }
            }
            else if (_outputKind == OutputKind.Rle)
            {
                if (_output.Length != 0)
                {
                    WriteUInt16(_output, _offset, _pendingStart);
                    WriteUInt16(_output, _offset + 2, _pendingEnd - _pendingStart);
                    _offset += 4;
                }
            }
            else
            {
                if (_output.Length != 0)
                    SetRange(_output, _pendingStart, _pendingEnd);
            }


            _hasPending = false;
        }
    }

    public readonly struct RoaringBitmapEnumerable : IEnumerable<ulong>
    {
        readonly ReadOnlyMemory<byte> _input;
        readonly ulong _addOffset;

        internal RoaringBitmapEnumerable(ReadOnlyMemory<byte> input, ulong addOffset)
        {
            _input = input;
            _addOffset = addOffset;
        }

        public Enumerator GetEnumerator()
        {
            return new(_input, _addOffset);
        }

        IEnumerator<ulong> IEnumerable<ulong>.GetEnumerator()
        {
            return GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    public readonly ref struct DenseBitmapEnumerable
    {
        readonly ReadOnlySpan<byte> _bitmap;
        readonly ulong _addOffset;

        internal DenseBitmapEnumerable(ReadOnlySpan<byte> bitmap, ulong addOffset)
        {
            _bitmap = bitmap;
            _addOffset = addOffset;
        }

        public DenseBitmapEnumerator GetEnumerator()
        {
            return new(_bitmap, _addOffset);
        }
    }

    public ref struct DenseBitmapEnumerator
    {
        readonly ReadOnlySpan<byte> _bitmap;
        readonly ulong _addOffset;
        int _index;
        ulong _bitmapWord;
        int _bitmapWordBitBase;

        internal DenseBitmapEnumerator(ReadOnlySpan<byte> bitmap, ulong addOffset)
        {
            _bitmap = bitmap;
            _addOffset = addOffset;
            _index = 0;
            _bitmapWord = 0;
            _bitmapWordBitBase = 0;
            Current = 0;
        }

        public ulong Current { get; private set; }

        public bool MoveNext()
        {
            while (_bitmapWord == 0)
            {
                if (_index >= BitmapSize) return false;
                _bitmapWord = ReadUInt64LittleEndianUnsafe(_bitmap, _index);
                _bitmapWordBitBase = _index * 8;
                _index += sizeof(ulong);
            }

            var bit = BitOperations.TrailingZeroCount(_bitmapWord);
            _bitmapWord &= _bitmapWord - 1;
            Current = _addOffset + (uint)(_bitmapWordBitBase + bit);
            return true;
        }

        public void Reset()
        {
            _index = 0;
            _bitmapWord = 0;
            _bitmapWordBitBase = 0;
            Current = 0;
        }
    }

    public struct Enumerator : IEnumerator<ulong>
    {
        readonly ReadOnlyMemory<byte> _input;
        readonly ulong _addOffset;
        readonly ContainerKind _kind;
        int _index;
        int _currentOffset;
        ulong _bitmapWord;
        int _bitmapWordBitBase;
        int _rleEnd;

        internal Enumerator(ReadOnlyMemory<byte> input, ulong addOffset)
        {
            _input = input;
            _addOffset = addOffset;
            _kind = ValidateAndGetKind(input.Span);
            _index = 0;
            _currentOffset = 1;
            _bitmapWord = 0;
            _bitmapWordBitBase = 0;
            _rleEnd = 0;
            Current = 0;
        }

        public ulong Current { get; private set; }

        readonly object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            var input = _input.Span;
            if (_kind == ContainerKind.Empty) return false;
            if (_kind == ContainerKind.Array) return MoveNextArray(input);
            if (_kind == ContainerKind.Rle) return MoveNextRle(input);
            return MoveNextBitmap(input);
        }

        bool MoveNextArray(ReadOnlySpan<byte> input)
        {
            if (_index >= input.Length) return false;
            Current = _addOffset + ReadUInt16(input, _index);
            _index += 2;
            return true;
        }

        bool MoveNextRle(ReadOnlySpan<byte> input)
        {
            if (_currentOffset <= _rleEnd)
            {
                Current = _addOffset + (uint)_currentOffset++;
                return true;
            }

            if (_index >= input.Length - 1) return false;
            _currentOffset = ReadUInt16LittleEndianUnsafe(input, _index);
            _rleEnd = _currentOffset + ReadUInt16LittleEndianUnsafe(input, _index + 2);
            _index += 4;
            Current = _addOffset + (uint)_currentOffset++;
            return true;
        }

        bool MoveNextBitmap(ReadOnlySpan<byte> input)
        {
            while (_bitmapWord == 0)
            {
                if (_index >= BitmapSize) return false;
                _bitmapWord = ReadUInt64LittleEndianUnsafe(input, _index);
                _bitmapWordBitBase = _index * 8;
                _index += sizeof(ulong);
            }

            var bit = BitOperations.TrailingZeroCount(_bitmapWord);
            _bitmapWord &= _bitmapWord - 1;
            Current = _addOffset + (uint)(_bitmapWordBitBase + bit);
            return true;
        }

        public void Reset()
        {
            _index = 0;
            _currentOffset = 1;
            _bitmapWord = 0;
            _bitmapWordBitBase = 0;
            _rleEnd = 0;
            Current = 0;
        }

        public readonly void Dispose()
        {
        }
    }
}
