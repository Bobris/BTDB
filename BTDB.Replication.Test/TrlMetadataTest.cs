using System;
using System.Collections.Generic;
using Xunit;

namespace BTDB.Replication.Test;

public class TrlMetadataTest
{
    [Fact]
    public void MetadataRejectsMissingAuthorityAndIncompleteContinuation()
    {
        Assert.Throws<FormatException>(() => TrlMetadata.Decode(new Dictionary<string, string> { ["unrelated"] = "1" }));
        Assert.Throws<FormatException>(() => TrlMetadata.Decode(new Dictionary<string, string>
            { ["btdb_term"] = "1", ["btdb_next"] = "next" }));
        var metadata = new TrlMetadata(3, new("branch/next", 5));
        Assert.Equal(metadata, TrlMetadata.Decode(metadata.Encode()));
    }
}
