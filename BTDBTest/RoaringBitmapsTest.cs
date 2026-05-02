using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using BTDB.Buffer;
using Xunit;

namespace BTDBTest;

public class RoaringBitmapsTest
{
    [Fact]
    public void SetUnsetAndGetBitWorkOnRawBitmap()
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];

        RoaringBitmaps.SetBit(bitmap, 0);
        RoaringBitmaps.SetBit(bitmap, 63);
        RoaringBitmaps.SetBit(bitmap, 65535);

        Assert.True(RoaringBitmaps.GetBit(bitmap, 0));
        Assert.True(RoaringBitmaps.GetBit(bitmap, 63));
        Assert.True(RoaringBitmaps.GetBit(bitmap, 65535));
        Assert.False(RoaringBitmaps.GetBit(bitmap, 64));

        RoaringBitmaps.UnsetBit(bitmap, 63);

        Assert.True(RoaringBitmaps.GetBit(bitmap, 0));
        Assert.False(RoaringBitmaps.GetBit(bitmap, 63));
        Assert.True(RoaringBitmaps.GetBit(bitmap, 65535));
    }

    [Fact]
    public void CompressChoosesEmptyArrayRleAndBitmapRepresentations()
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        var output = new byte[RoaringBitmaps.BitmapSize];

        Assert.Equal(0, RoaringBitmaps.Compress(bitmap, output));

        RoaringBitmaps.SetBit(bitmap, 1);
        RoaringBitmaps.SetBit(bitmap, 42);
        RoaringBitmaps.SetBit(bitmap, 65535);
        var length = RoaringBitmaps.Compress(bitmap, output);
        Assert.Equal(6, length);
        Assert.Equal(new byte[] { 1, 0, 42, 0, 255, 255 }, output[..length]);

        Array.Clear(bitmap);
        for (var i = 10; i <= 100; i++) RoaringBitmaps.SetBit(bitmap, (ushort)i);
        length = RoaringBitmaps.Compress(bitmap, output);
        Assert.Equal(5, length);
        Assert.Equal(new byte[] { 10, 0, 90, 0, 0 }, output[..length]);

        Array.Clear(bitmap);
        for (var i = 0; i < 4096; i++) RoaringBitmaps.SetBit(bitmap, (ushort)(i * 2));
        length = RoaringBitmaps.Compress(bitmap, output);
        Assert.Equal(RoaringBitmaps.BitmapSize, length);
        Assert.True(output.AsSpan(0, length).SequenceEqual(bitmap));
    }

    [Fact]
    public void FullBitmapCompressesToSingleRleRun()
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        bitmap.AsSpan().Fill(0xff);
        var output = new byte[RoaringBitmaps.BitmapSize];

        var length = RoaringBitmaps.Compress(bitmap, output);

        Assert.Equal(5, length);
        Assert.Equal(new byte[] { 0, 0, 255, 255, 0 }, output[..length]);
    }

    [Fact]
    public void BooleanOperationsWorkAcrossRepresentations()
    {
        var leftBitmap = new byte[RoaringBitmaps.BitmapSize];
        var rightBitmap = new byte[RoaringBitmaps.BitmapSize];
        for (var i = 10; i <= 20; i++) RoaringBitmaps.SetBit(leftBitmap, (ushort)i);
        RoaringBitmaps.SetBit(leftBitmap, 1000);
        RoaringBitmaps.SetBit(leftBitmap, 65535);
        for (var i = 15; i <= 25; i++) RoaringBitmaps.SetBit(rightBitmap, (ushort)i);
        RoaringBitmaps.SetBit(rightBitmap, 1000);
        RoaringBitmaps.SetBit(rightBitmap, 50000);

        var left = new byte[RoaringBitmaps.BitmapSize];
        var right = new byte[RoaringBitmaps.BitmapSize];
        var leftLength = RoaringBitmaps.Compress(leftBitmap, left);
        var rightLength = RoaringBitmaps.Compress(rightBitmap, right);
        var output = new byte[RoaringBitmaps.BitmapSize];

        var andLength = RoaringBitmaps.And(left.AsSpan(0, leftLength), right.AsSpan(0, rightLength), output);
        Assert.Equal(new byte[] { 15, 0, 5, 0, 232, 3, 0, 0, 0 }, output[..andLength]);
        AssertResult(leftBitmap, rightBitmap, output, andLength,
            (l, r) => (byte)(l & r));
        AssertResult(leftBitmap, rightBitmap, output, RoaringBitmaps.Or(left.AsSpan(0, leftLength), right.AsSpan(0, rightLength), output),
            (l, r) => (byte)(l | r));
        AssertResult(leftBitmap, rightBitmap, output, RoaringBitmaps.AndNot(left.AsSpan(0, leftLength), right.AsSpan(0, rightLength), output),
            (l, r) => (byte)(l & ~r));
    }

    [Fact]
    public void DenseBitmapOperationsWorkWithMisalignedSpans()
    {
        var leftStorage = new byte[RoaringBitmaps.BitmapSize + 1];
        var rightStorage = new byte[RoaringBitmaps.BitmapSize + 2];
        var outputStorage = new byte[RoaringBitmaps.BitmapSize + 3];
        var left = leftStorage.AsSpan(1, RoaringBitmaps.BitmapSize);
        var right = rightStorage.AsSpan(2, RoaringBitmaps.BitmapSize);
        var output = outputStorage.AsSpan(3, RoaringBitmaps.BitmapSize);
        for (var i = 0; i < RoaringBitmaps.BitmapSize; i++)
        {
            left[i] = (byte)(i * 37 + 11);
            right[i] = (byte)(255 - i * 13);
        }

        var length = RoaringBitmaps.And(left, right, output);
        AssertResult(left.ToArray(), right.ToArray(), output.ToArray(), length, (l, r) => (byte)(l & r));

        length = RoaringBitmaps.Or(left, right, output);
        AssertResult(left.ToArray(), right.ToArray(), output.ToArray(), length, (l, r) => (byte)(l | r));

        length = RoaringBitmaps.AndNot(left, right, output);
        AssertResult(left.ToArray(), right.ToArray(), output.ToArray(), length, (l, r) => (byte)(l & ~r));

        var actual = new byte[RoaringBitmaps.BitmapSize + 1];
        left.CopyTo(actual.AsSpan(1));
        RoaringBitmaps.OrNot(actual.AsSpan(1), right);
        AssertBitmap(actual[1..], left.ToArray(), right.ToArray(), (l, r) => (byte)(l | ~r));

        actual = new byte[RoaringBitmaps.BitmapSize + 1];
        left.CopyTo(actual.AsSpan(1));
        RoaringBitmaps.Not(actual.AsSpan(1));
        for (var i = 0; i < RoaringBitmaps.BitmapSize; i++)
            Assert.Equal((byte)~left[i], actual[i + 1]);
    }

    [Fact]
    public unsafe void DenseBitmapOperationsWorkWithAlignedSpans()
    {
        var vectorSize = Vector<byte>.Count;
        var leftStorage = new byte[RoaringBitmaps.BitmapSize + vectorSize];
        var rightStorage = new byte[RoaringBitmaps.BitmapSize + vectorSize];
        var outputStorage = new byte[RoaringBitmaps.BitmapSize + vectorSize];
        fixed (byte* leftPtr = leftStorage)
        fixed (byte* rightPtr = rightStorage)
        fixed (byte* outputPtr = outputStorage)
        {
            var left = leftStorage.AsSpan(AlignedOffset(leftPtr, vectorSize), RoaringBitmaps.BitmapSize);
            var right = rightStorage.AsSpan(AlignedOffset(rightPtr, vectorSize), RoaringBitmaps.BitmapSize);
            var output = outputStorage.AsSpan(AlignedOffset(outputPtr, vectorSize), RoaringBitmaps.BitmapSize);
            for (var i = 0; i < RoaringBitmaps.BitmapSize; i++)
            {
                left[i] = (byte)(i * 17 + 3);
                right[i] = (byte)(i * 29 + 5);
            }

            var length = RoaringBitmaps.And(left, right, output);
            AssertResult(left.ToArray(), right.ToArray(), output.ToArray(), length, (l, r) => (byte)(l & r));

            length = RoaringBitmaps.Or(left, right, output);
            AssertResult(left.ToArray(), right.ToArray(), output.ToArray(), length, (l, r) => (byte)(l | r));

            length = RoaringBitmaps.AndNot(left, right, output);
            AssertResult(left.ToArray(), right.ToArray(), output.ToArray(), length, (l, r) => (byte)(l & ~r));

            left.CopyTo(output);
            RoaringBitmaps.OrNot(output, right);
            AssertBitmap(output.ToArray(), left.ToArray(), right.ToArray(), (l, r) => (byte)(l | ~r));
        }
    }

    [Fact]
    public void NotInvertsEncodedInput()
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        for (var i = 100; i <= 200; i++) RoaringBitmaps.SetBit(bitmap, (ushort)i);
        var encoded = new byte[RoaringBitmaps.BitmapSize];
        var encodedLength = RoaringBitmaps.Compress(bitmap, encoded);
        var output = new byte[RoaringBitmaps.BitmapSize];

        var length = RoaringBitmaps.Not(encoded.AsSpan(0, encodedLength), output);

        var decompressed = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.ToBitmap(output.AsSpan(0, length), decompressed);
        for (var i = 0; i < 65536; i++)
            Assert.Equal(!RoaringBitmaps.GetBit(bitmap, (ushort)i), RoaringBitmaps.GetBit(decompressed, (ushort)i));
    }

    [Fact]
    public void InPlaceBitmapOperationsMatchRawBitmapReference()
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        var operand = new byte[RoaringBitmaps.BitmapSize];
        var encoded = new byte[RoaringBitmaps.BitmapSize];
        FillRandom(new Random(1), bitmap, 2);

        var inverted = (byte[])bitmap.Clone();
        RoaringBitmaps.Not(inverted);
        for (var i = 0; i < RoaringBitmaps.BitmapSize; i++)
            Assert.Equal((byte)~bitmap[i], inverted[i]);

        foreach (var pattern in new[] { -1, 0, 1, 2, 3 })
        {
            if (pattern < 0)
                Array.Clear(operand);
            else
                FillRandom(new Random(100 + pattern), operand, pattern);
            var encodedLength = RoaringBitmaps.Compress(operand, encoded);
            var encodedSpan = encoded.AsSpan(0, encodedLength);

            var actual = (byte[])bitmap.Clone();
            RoaringBitmaps.Or(actual, encodedSpan);
            AssertBitmap(actual, bitmap, operand, (l, r) => (byte)(l | r));

            actual = (byte[])bitmap.Clone();
            RoaringBitmaps.And(actual, encodedSpan);
            AssertBitmap(actual, bitmap, operand, (l, r) => (byte)(l & r));

            actual = (byte[])bitmap.Clone();
            RoaringBitmaps.OrNot(actual, encodedSpan);
            AssertBitmap(actual, bitmap, operand, (l, r) => (byte)(l | ~r));

            actual = (byte[])bitmap.Clone();
            RoaringBitmaps.AndNot(actual, encodedSpan);
            AssertBitmap(actual, bitmap, operand, (l, r) => (byte)(l & ~r));
        }
    }

    [Fact]
    public void RandomOperationsMatchRawBitmapReference()
    {
        var random = new Random(123456);
        var leftBitmap = new byte[RoaringBitmaps.BitmapSize];
        var rightBitmap = new byte[RoaringBitmaps.BitmapSize];
        var leftEncoded = new byte[RoaringBitmaps.BitmapSize];
        var rightEncoded = new byte[RoaringBitmaps.BitmapSize];
        var output = new byte[RoaringBitmaps.BitmapSize];

        for (var iteration = 0; iteration < 50; iteration++)
        {
            FillRandom(random, leftBitmap, iteration);
            FillRandom(random, rightBitmap, 49 - iteration);
            var leftLength = RoaringBitmaps.Compress(leftBitmap, leftEncoded);
            var rightLength = RoaringBitmaps.Compress(rightBitmap, rightEncoded);

            AssertResult(leftBitmap, rightBitmap, output,
                RoaringBitmaps.And(leftEncoded.AsSpan(0, leftLength), rightEncoded.AsSpan(0, rightLength), output),
                (l, r) => (byte)(l & r));
            AssertResult(leftBitmap, rightBitmap, output,
                RoaringBitmaps.Or(leftEncoded.AsSpan(0, leftLength), rightEncoded.AsSpan(0, rightLength), output),
                (l, r) => (byte)(l | r));
            AssertResult(leftBitmap, rightBitmap, output,
                RoaringBitmaps.AndNot(leftEncoded.AsSpan(0, leftLength), rightEncoded.AsSpan(0, rightLength), output),
                (l, r) => (byte)(l & ~r));
        }
    }

    [Fact]
    public void InvalidEncodedInputsThrow()
    {
        var output = new byte[RoaringBitmaps.BitmapSize];

        Assert.Throws<ArgumentException>(() => RoaringBitmaps.ToBitmap(new byte[] { 1 }, output));
        Assert.Throws<ArgumentException>(() => RoaringBitmaps.ToBitmap(new byte[] { 2, 0, 1, 0 }, output));
        Assert.Throws<ArgumentException>(() => RoaringBitmaps.ToBitmap(new byte[] { 2, 0, 1, 0, 1 }, output));
        Assert.Throws<ArgumentException>(() => RoaringBitmaps.ToBitmap(new byte[] { 10, 0, 9, 0 }, output));
        Assert.Throws<ArgumentException>(() => RoaringBitmaps.ToBitmap(new byte[] { 255, 255, 1, 0, 0 }, output));
        Assert.Throws<ArgumentException>(() => RoaringBitmaps.ToBitmap(new byte[] { 10, 0, 10, 0, 20, 0, 1, 0, 0 }, output));
        Assert.Throws<ArgumentException>(() => RoaringBitmaps.ToBitmap(new byte[RoaringBitmaps.BitmapSize + 1], output));
        Assert.Throws<ArgumentException>(() => RoaringBitmaps.Compress([], output));
        Assert.Throws<ArgumentException>(() => RoaringBitmaps.Compress(output, []));
        Assert.Throws<ArgumentException>(() => RoaringBitmaps.GetBit([], 0));
        Assert.Throws<ArgumentException>(() => RoaringBitmaps.SetBit([], 0));
        Assert.Throws<ArgumentException>(() => RoaringBitmaps.UnsetBit([], 0));
    }

    [Fact]
    public void ContainsWorksForEveryRepresentation()
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.SetBit(bitmap, 7);
        RoaringBitmaps.SetBit(bitmap, 700);
        RoaringBitmaps.SetBit(bitmap, 60000);

        Assert.False(RoaringBitmaps.Contains([], 7));
        Assert.True(RoaringBitmaps.Contains(bitmap, 700));
        Assert.False(RoaringBitmaps.Contains(bitmap, 701));
        Assert.True(RoaringBitmaps.Contains(new byte[] { 7, 0, 188, 2, 96, 234 }, 700));
        Assert.False(RoaringBitmaps.Contains(new byte[] { 7, 0, 188, 2, 96, 234 }, 699));
        Assert.False(RoaringBitmaps.Contains(new byte[] { 7, 0, 188, 2, 96, 234 }, 65535));
        Assert.True(RoaringBitmaps.Contains(new byte[] { 10, 0, 5, 0, 232, 3, 10, 0, 0 }, 1005));
        Assert.False(RoaringBitmaps.Contains(new byte[] { 10, 0, 5, 0, 232, 3, 10, 0, 0 }, 9));
        Assert.False(RoaringBitmaps.Contains(new byte[] { 10, 0, 5, 0, 232, 3, 10, 0, 0 }, 2000));
    }

    [Fact]
    public void EmptyInputsAndDirectBitmapCopyAreHandledWithoutExpansion()
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.SetBit(bitmap, 1);
        RoaringBitmaps.SetBit(bitmap, 4096);
        var output = new byte[RoaringBitmaps.BitmapSize];
        var decompressed = new byte[RoaringBitmaps.BitmapSize];

        RoaringBitmaps.ToBitmap([], decompressed);
        Assert.True(decompressed.AsSpan().SequenceEqual(new byte[RoaringBitmaps.BitmapSize]));
        RoaringBitmaps.ToBitmap(bitmap, decompressed);
        Assert.True(decompressed.AsSpan().SequenceEqual(bitmap));

        Assert.Equal(0, RoaringBitmaps.And([], bitmap, output));
        Assert.Equal(0, RoaringBitmaps.And(bitmap, [], output));
        Assert.Equal(0, RoaringBitmaps.AndNot([], bitmap, output));

        var length = RoaringBitmaps.Or([], bitmap, output);
        Assert.Equal(RoaringBitmaps.BitmapSize, length);
        Assert.True(output.AsSpan().SequenceEqual(bitmap));

        length = RoaringBitmaps.Or(new byte[] { 1, 0, 2, 0 }, [], output);
        Assert.Equal(4, length);
        Assert.Equal(new byte[] { 1, 0, 2, 0 }, output[..length]);

        length = RoaringBitmaps.AndNot(bitmap, [], output);
        Assert.Equal(RoaringBitmaps.BitmapSize, length);
        Assert.True(output.AsSpan().SequenceEqual(bitmap));

        var largestValidRle = CreateSteppedRle(0, 8, 2047);
        length = RoaringBitmaps.Or([], largestValidRle, output);
        Assert.Equal(largestValidRle.Length, length);
        Assert.True(output.AsSpan(0, length).SequenceEqual(largestValidRle));
    }

    [Fact]
    public void NotHandlesEmptyAndRawBitmap()
    {
        var output = new byte[RoaringBitmaps.BitmapSize];
        var length = RoaringBitmaps.Not([], output);
        Assert.Equal(5, length);
        Assert.Equal(new byte[] { 0, 0, 255, 255, 0 }, output[..length]);

        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.SetBit(bitmap, 42);
        length = RoaringBitmaps.Not(bitmap, output);
        var decompressed = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.ToBitmap(output.AsSpan(0, length), decompressed);
        Assert.False(RoaringBitmaps.GetBit(decompressed, 42));
        Assert.True(RoaringBitmaps.GetBit(decompressed, 41));
        Assert.True(RoaringBitmaps.GetBit(decompressed, 43));
    }

    [Fact]
    public void CompressAllowsOverlappingInputAndOutput()
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.SetBit(bitmap, 3);
        RoaringBitmaps.SetBit(bitmap, 400);

        var length = RoaringBitmaps.Compress(bitmap, bitmap);

        Assert.Equal(4, length);
        Assert.Equal(new byte[] { 3, 0, 144, 1 }, bitmap[..length]);
    }

    [Fact]
    public void BitmapAndArraySpecializedPathsWork()
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.SetBit(bitmap, 5);
        RoaringBitmaps.SetBit(bitmap, 10);
        RoaringBitmaps.SetBit(bitmap, 20);
        var array = new byte[] { 1, 0, 5, 0, 20, 0 };
        var output = new byte[RoaringBitmaps.BitmapSize];

        Assert.Equal(new byte[] { 5, 0, 20, 0 },
            output[..RoaringBitmaps.And(array, bitmap, output)]);
        Assert.Equal(new byte[] { 5, 0, 20, 0 },
            output[..RoaringBitmaps.And(bitmap, array, output)]);
        Assert.Equal(new byte[] { 1, 0 },
            output[..RoaringBitmaps.AndNot(array, bitmap, output)]);
        Assert.Equal(0, RoaringBitmaps.AndNot(new byte[] { 5, 0 }, bitmap, output));

        AssertResult(bitmap, ArrayToBitmap(array), output, RoaringBitmaps.Or(bitmap, array, output),
            (l, r) => (byte)(l | r));
        AssertResult(bitmap, ArrayToBitmap(array), output, RoaringBitmaps.AndNot(bitmap, array, output),
            (l, r) => (byte)(l & ~r));
    }

    [Fact]
    public void BitmapAndRlePathsWorkInBothOrders()
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        for (var i = 10; i <= 20; i++) RoaringBitmaps.SetBit(bitmap, (ushort)i);
        RoaringBitmaps.SetBit(bitmap, 500);
        var rle = new byte[] { 15, 0, 10, 0, 244, 1, 0, 0, 0 };
        var output = new byte[RoaringBitmaps.BitmapSize];

        AssertResult(bitmap, RleToBitmap(rle), output, RoaringBitmaps.And(bitmap, rle, output), (l, r) => (byte)(l & r));
        AssertResult(bitmap, RleToBitmap(rle), output, RoaringBitmaps.And(rle, bitmap, output), (l, r) => (byte)(l & r));
        AssertResult(bitmap, RleToBitmap(rle), output, RoaringBitmaps.Or(bitmap, rle, output), (l, r) => (byte)(l | r));
        AssertResult(bitmap, RleToBitmap(rle), output, RoaringBitmaps.Or(rle, bitmap, output), (l, r) => (byte)(l | r));
        AssertResult(bitmap, RleToBitmap(rle), output, RoaringBitmaps.AndNot(bitmap, rle, output), (l, r) => (byte)(l & ~r));
        AssertResult(RleToBitmap(rle), bitmap, output, RoaringBitmaps.AndNot(rle, bitmap, output), (l, r) => (byte)(l & ~r));
    }

    [Fact]
    public void BitmapOperationsCoverOrAndAndNot()
    {
        var left = new byte[RoaringBitmaps.BitmapSize];
        var right = new byte[RoaringBitmaps.BitmapSize];
        left[0] = 0b_0000_1111;
        right[0] = 0b_0011_0011;
        left[1024] = 0xff;
        right[1024] = 0x0f;
        var output = new byte[RoaringBitmaps.BitmapSize];

        AssertResult(left, right, output, RoaringBitmaps.And(left, right, output), (l, r) => (byte)(l & r));
        AssertResult(left, right, output, RoaringBitmaps.Or(left, right, output), (l, r) => (byte)(l | r));
        AssertResult(left, right, output, RoaringBitmaps.AndNot(left, right, output), (l, r) => (byte)(l & ~r));
    }

    [Fact]
    public void IntervalOperationsCanReturnEmptyArrayRleAndBitmap()
    {
        var output = new byte[RoaringBitmaps.BitmapSize];

        Assert.Equal(0, RoaringBitmaps.And(new byte[] { 1, 0 }, new byte[] { 2, 0 }, output));
        Assert.Equal(0, RoaringBitmaps.And(new byte[] { 1, 0, 0, 0, 0 }, new byte[] { 3, 0, 0, 0, 0 }, output));
        Assert.Equal(new byte[] { 255, 255 }, output[..RoaringBitmaps.And(new byte[] { 255, 255 }, new byte[] { 255, 255 }, output)]);
        Assert.Equal(new byte[] { 5, 0 }, output[..RoaringBitmaps.And(new byte[] { 1, 0, 5, 0 }, new byte[] { 5, 0 }, output)]);
        Assert.Equal(new byte[] { 5, 0 }, output[..RoaringBitmaps.And(new byte[] { 5, 0 }, new byte[] { 1, 0, 5, 0 }, output)]);
        Assert.Equal(new byte[] { 1, 0, 3, 0 },
            output[..RoaringBitmaps.AndNot(new byte[] { 1, 0, 3, 0, 5, 0 }, new byte[] { 2, 0, 5, 0 }, output)]);
        Assert.Equal(new byte[] { 1, 0, 2, 0 },
            output[..RoaringBitmaps.AndNot(new byte[] { 0, 0, 1, 0, 2, 0 }, new byte[] { 0, 0 }, output)]);
        Assert.Equal(new byte[] { 10, 0, 10, 0, 0 }, output[..RoaringBitmaps.Or(
            new byte[] { 10, 0, 2, 0, 0 }, new byte[] { 13, 0, 7, 0, 0 }, output)]);
        Assert.Equal(new byte[] { 5, 0, 10, 0, 11, 0, 12, 0 }, output[..RoaringBitmaps.Or(
            new byte[] { 5, 0 }, new byte[] { 10, 0, 2, 0, 0 }, output)]);

        var left = CreateSteppedRle(0, 8, 2047, 1);
        var right = CreateSteppedRle(4, 8, 2047, 1);
        var length = RoaringBitmaps.Or(left, right, output);
        Assert.Equal(RoaringBitmaps.BitmapSize, length);
        for (var i = 0; i < 2047; i++) Assert.Equal(0x33, output[i]);
        for (var i = 2047; i < RoaringBitmaps.BitmapSize; i++) Assert.Equal(0, output[i]);
    }

    [Fact]
    public void AndNotHandlesRightRunEndingAtLastBit()
    {
        var output = new byte[RoaringBitmaps.BitmapSize];
        var left = new byte[] { 10, 0, 245, 255, 0 };
        var right = new byte[] { 20, 0, 235, 255, 0 };

        var length = RoaringBitmaps.AndNot(left, right, output);

        Assert.Equal(new byte[] { 10, 0, 9, 0, 0 }, output[..length]);
    }

    [Fact]
    public void EnumerateReturnsOffsetsFromEveryRepresentation()
    {
        Assert.Empty(RoaringBitmaps.Enumerate(ReadOnlyMemory<byte>.Empty, 123));
        Assert.Equal([101ul, 142ul, 65635ul],
            RoaringBitmaps.Enumerate(new byte[] { 1, 0, 42, 0, 255, 255 }, 100).ToArray());
        Assert.Equal([15ul, 16ul, 17ul, 100ul],
            RoaringBitmaps.Enumerate(new byte[] { 10, 0, 2, 0, 95, 0, 0, 0, 0 }, 5).ToArray());

        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.SetBit(bitmap, 0);
        RoaringBitmaps.SetBit(bitmap, 63);
        RoaringBitmaps.SetBit(bitmap, 64);
        RoaringBitmaps.SetBit(bitmap, 65535);
        Assert.Equal([1000ul, 1063ul, 1064ul, 66535ul], RoaringBitmaps.Enumerate(bitmap, 1000).ToArray());
    }

    [Fact]
    public void ConcreteEnumeratorCanBeUsedWithoutInterface()
    {
        var enumerable = RoaringBitmaps.Enumerate(new byte[] { 7, 0, 8, 0 }, ulong.MaxValue - 10);
        var enumerator = enumerable.GetEnumerator();

        Assert.True(enumerator.MoveNext());
        Assert.Equal(ulong.MaxValue - 3, enumerator.Current);
        Assert.True(enumerator.MoveNext());
        Assert.Equal(ulong.MaxValue - 2, enumerator.Current);
        Assert.False(enumerator.MoveNext());
        Assert.False(enumerator.MoveNext());
    }

    [Fact]
    public void EnumerableCanBeUsedThroughInterface()
    {
        IEnumerable<ulong> enumerable = RoaringBitmaps.Enumerate(new byte[] { 2, 0, 3, 0 }, 10);
        using var enumerator = enumerable.GetEnumerator();

        Assert.True(enumerator.MoveNext());
        Assert.Equal(12ul, enumerator.Current);
        Assert.True(enumerator.MoveNext());
        Assert.Equal(13ul, enumerator.Current);
        Assert.False(enumerator.MoveNext());
    }

    [Fact]
    public void EnumeratorSupportsNonGenericInterfaceResetAndDispose()
    {
        var enumerable = RoaringBitmaps.Enumerate(new byte[] { 2, 0, 3, 0 }, 10);
        var nonGenericEnumerable = (System.Collections.IEnumerable)enumerable;
        var enumerator = nonGenericEnumerable.GetEnumerator();

        Assert.True(enumerator.MoveNext());
        Assert.Equal(12ul, enumerator.Current);
        enumerator.Reset();
        Assert.True(enumerator.MoveNext());
        Assert.Equal(12ul, enumerator.Current);
        ((IDisposable)enumerator).Dispose();
    }

    [Fact]
    public void EnumerateBitmapReturnsOffsetsFromDenseReadOnlySpan()
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.SetBit(bitmap, 0);
        RoaringBitmaps.SetBit(bitmap, 31);
        RoaringBitmaps.SetBit(bitmap, 32);
        RoaringBitmaps.SetBit(bitmap, 4096);
        RoaringBitmaps.SetBit(bitmap, 65535);
        var result = new List<ulong>();

        foreach (var value in RoaringBitmaps.EnumerateBitmap(bitmap.AsSpan(), 100))
            result.Add(value);

        Assert.Equal([100ul, 131ul, 132ul, 4196ul, 65635ul], result);
    }

    [Fact]
    public void DenseBitmapSpanEnumeratorCanBeUsedDirectlyAndReset()
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.SetBit(bitmap, 64);
        RoaringBitmaps.SetBit(bitmap, 65);
        var enumerator = RoaringBitmaps.EnumerateBitmap(bitmap.AsSpan(), 10).GetEnumerator();

        Assert.True(enumerator.MoveNext());
        Assert.Equal(74ul, enumerator.Current);
        Assert.True(enumerator.MoveNext());
        Assert.Equal(75ul, enumerator.Current);
        Assert.False(enumerator.MoveNext());
        enumerator.Reset();
        Assert.True(enumerator.MoveNext());
        Assert.Equal(74ul, enumerator.Current);
    }

    [Fact]
    public void EnumerateBitmapRejectsNonDenseInput()
    {
        Assert.Throws<ArgumentException>(() => RoaringBitmaps.EnumerateBitmap([], 0));
    }

    static void FillRandom(Random random, byte[] bitmap, int pattern)
    {
        Array.Clear(bitmap);
        switch (pattern % 5)
        {
            case 0:
                for (var i = 0; i < 300; i++) RoaringBitmaps.SetBit(bitmap, (ushort)random.Next(65536));
                break;
            case 1:
                for (var start = 0; start < 65536; start += 2048)
                    for (var i = start; i < start + 900; i++)
                        RoaringBitmaps.SetBit(bitmap, (ushort)i);
                break;
            case 2:
                random.NextBytes(bitmap);
                break;
            case 3:
                bitmap.AsSpan().Fill(0xff);
                for (var i = 0; i < 2000; i++) RoaringBitmaps.UnsetBit(bitmap, (ushort)random.Next(65536));
                break;
        }
    }

    static void AssertResult(byte[] leftBitmap, byte[] rightBitmap, byte[] output, int length, Func<byte, byte, byte> op)
    {
        var decompressed = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.ToBitmap(output.AsSpan(0, length), decompressed);
        AssertBitmap(decompressed, leftBitmap, rightBitmap, op);
    }

    static void AssertBitmap(byte[] actual, byte[] leftBitmap, byte[] rightBitmap, Func<byte, byte, byte> op)
    {
        for (var i = 0; i < RoaringBitmaps.BitmapSize; i++)
            Assert.Equal(op(leftBitmap[i], rightBitmap[i]), actual[i]);
    }

    static unsafe int AlignedOffset(byte* ptr, int alignment)
    {
        var mask = (nuint)(alignment - 1);
        return (int)(((nuint)alignment - ((nuint)ptr & mask)) & mask);
    }

    static byte[] RleToBitmap(byte[] rle)
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.ToBitmap(rle, bitmap);
        return bitmap;
    }

    static byte[] ArrayToBitmap(byte[] array)
    {
        var bitmap = new byte[RoaringBitmaps.BitmapSize];
        RoaringBitmaps.ToBitmap(array, bitmap);
        return bitmap;
    }

    static byte[] CreateSteppedRle(int first, int step, int runCount, ushort lengthMinusOne = 0)
    {
        var rle = new byte[runCount * 4 + 1];
        var offset = 0;
        for (var i = 0; i < runCount; i++)
        {
            var value = first + i * step;
            rle[offset++] = (byte)value;
            rle[offset++] = (byte)(value >> 8);
            rle[offset++] = (byte)lengthMinusOne;
            rle[offset++] = (byte)(lengthMinusOne >> 8);
        }

        rle[offset] = 0;
        return rle;
    }

    static byte[] CreateArrayRange(int start, int count)
    {
        var array = new byte[count * 2];
        for (var i = 0; i < count; i++)
        {
            var value = start + i;
            array[i * 2] = (byte)value;
            array[i * 2 + 1] = (byte)(value >> 8);
        }

        return array;
    }
}
