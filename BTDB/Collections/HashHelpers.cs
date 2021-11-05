// Idea taken from DictionarySlim in .NetCore with following license:
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Runtime.CompilerServices;

namespace BTDB.Collections;

static class HashHelpers
{
    internal static readonly int[] RefDictionarySizeOneIntArray = new int[1];

    internal static int PowerOf2(int v)
    {
        if ((v & (v - 1)) == 0) return v;
        int i = 2;
        while (i < v) i <<= 1;
        return i;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void ThrowInvalidOperationException_ConcurrentOperationsNotSupported()
    {
        throw new InvalidOperationException("Concurrent Operations Not Supported");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void ThrowKeyArgumentNullException()
    {
        throw new ArgumentNullException("key");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void ThrowCapacityArgumentOutOfRangeException()
    {
        throw new ArgumentOutOfRangeException("capacity");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static bool ThrowNotSupportedException()
    {
        throw new NotSupportedException();
    }
}
