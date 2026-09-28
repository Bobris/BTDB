using System;
using Xunit;

namespace BTDB.Replication.Test;

public class TrlFileNameTest
{
    [Theory]
    [InlineData(1u)]
    [InlineData(42u)]
    [InlineData(uint.MaxValue)]
    public void NativeIdUsesOneSharedName(uint id) => Assert.Equal(id, TrlFileName.FileIdFromKey(TrlFileName.Key(id)));

    [Theory]
    [InlineData("0.trl")]
    [InlineData("01.trl")]
    [InlineData("7")]
    [InlineData("7.pvl")]
    [InlineData("+7.trl")]
    [InlineData("4294967296.trl")]
    [InlineData("../7.trl")]
    [InlineData("term1/7.trl")]
    [InlineData("files/7.trl")]
    public void RejectNoncanonicalNames(string key) => Assert.Throws<FormatException>(() => TrlFileName.FileIdFromKey(key));
}
