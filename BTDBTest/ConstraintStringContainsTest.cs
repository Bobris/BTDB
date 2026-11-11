using System;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class ConstraintStringContainsTest
{
    static readonly string?[] Values =
    [
        null, "", "a", "~", "\u007f", "\0", "abc", "a\0b", "recipient.name@example.com", "abc\u007fdef",
        "přílišžluťoučký", "kůň a@b.cd", "a@b.cd kůň", "emoji \U0001F600 here", new string('x', 600) + "needle",
        "needle" + new string('č', 600)
    ];

    static readonly string[] Needles =
        ["a", "~", "\u007f", "\0", "abc", "a\0b", "@example.com", "name", "ů", "kůň", "\U0001F600", "needle", "zzz"];

    [Fact]
    public void MatchesStringContainsAndSkipsWholeValue()
    {
        var writer = new MemWriter();
        foreach (var value in Values) writer.WriteStringOrdered(value);
        writer.WriteVUInt32(12345);
        var data = writer.GetSpan().ToArray();
        foreach (var needle in Needles)
        {
            var constraint = new ConstraintStringContains(needle);
            var reader = MemReader.CreateFromReadOnlyMemory(data);
            foreach (var value in Values)
            {
                var expected = value != null && value.Contains(needle, StringComparison.Ordinal);
                Assert.Equal(expected ? IConstraint.MatchResult.Yes : IConstraint.MatchResult.No,
                    constraint.Match(ref reader, new()));
            }

            Assert.Equal(12345u, reader.ReadVUInt32());
            Assert.True(reader.Eof);
        }
    }
}
